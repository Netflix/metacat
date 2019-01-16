/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */


package com.netflix.metacat.main.services.search;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.TransportException;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Utility class for index, update, delete metacat doc from elastic search.
 */
@Slf4j
public class ElasticSearchUtilImpl implements ElasticSearchUtil {
    private static final Retryer<Void> RETRY_ES_PUBLISH = RetryerBuilder.<Void>newBuilder()
        .retryIfExceptionOfType(FailedNodeException.class)
        .retryIfExceptionOfType(NodeClosedException.class)
        .retryIfExceptionOfType(NoNodeAvailableException.class)
        .retryIfExceptionOfType(ReceiveTimeoutTransportException.class)
        .retryIfExceptionOfType(TransportException.class)
        .retryIfExceptionOfType(ElasticsearchTimeoutException.class)
        .retryIfExceptionOfType(EsRejectedExecutionException.class)
        .retryIfExceptionOfType(CancellableThreads.ExecutionCancelledException.class)
        .withWaitStrategy(WaitStrategies.incrementingWait(10, TimeUnit.MILLISECONDS, 30, TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
        .build();
    private static final int NO_OF_CONFLICT_RETRIES = 3;
    private final Client client;
    private final String esIndex;
    private final Config config;
    private final MetacatJson metacatJson;
    private XContentType contentType = Requests.INDEX_CONTENT_TYPE;
    private final MeterRegistry registry;
    private final TimeValue esCallTimeout;
    private final TimeValue esBulkCallTimeout;

    /**
     * Constructor.
     *
     * @param client              elastic search client
     * @param config              config
     * @param metacatJson         json utility
     * @param registry            micrometer registry
     */
    public ElasticSearchUtilImpl(
        @Nullable final Client client,
        final Config config,
        final MetacatJson metacatJson,
        final MeterRegistry registry) {
        this.config = config;
        this.client = client;
        this.metacatJson = metacatJson;
        this.esIndex = config.getEsIndex();
        this.registry = registry;
        this.esCallTimeout = TimeValue.timeValueSeconds(config.getElasticSearchCallTimeout());
        this.esBulkCallTimeout = TimeValue.timeValueSeconds(config.getElasticSearchBulkCallTimeout());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final String type, final String id) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                client.prepareDelete(esIndex, type, id).execute().actionGet(esCallTimeout);
                return null;
            });
        } catch (Exception e) {
            handleException("ElasticSearchUtil.delete", type, id, e,
                Metrics.CounterElasticSearchDelete.getMetricName());
        }
    }

    private void handleException(final String request,
                                 final String type,
                                 final String id,
                                 final Exception exception,
                                 final String metricName) {
        log.error("Failed {} metadata of type {} with id {}. {}", request, type, id, exception);
        String exceptionName = exception.getClass().getSimpleName();
        if (exception instanceof RetryException) {
            final Throwable error = ((RetryException) exception).getLastFailedAttempt().getExceptionCause();
            if (error != null) {
                exceptionName = error.getClass().getSimpleName();
            }
        }
        final Set<Tag> tags = ImmutableSet
            .<Tag>builder().add(Tag.of("status", "failure"))
            .add(Tag.of("name", id)).add(Tag.of("exception", exceptionName)).build();
        registry.counter(metricName, tags).increment();
        log(request, type, id, null, exception.getMessage(), exception, true);
    }

    private void handleException(final String request,
                                 final String type,
                                 final List<String> ids,
                                 final Exception exception,
                                 final String metricName) {
        log.error("Failed {} metadata of type {} with ids {}. {}", request, type, ids, exception);
        String exceptionName = exception.getClass().getSimpleName();
        if (exception instanceof RetryException) {
            final Throwable error = ((RetryException) exception).getLastFailedAttempt().getExceptionCause();
            if (error != null) {
                exceptionName = error.getClass().getSimpleName();
            }
        }
        final ImmutableSet<Tag> tags = ImmutableSet
            .<Tag>builder().add(Tag.of("status", "failure")).add(Tag.of("exception", exceptionName)).build();
        registry.counter(metricName, tags).increment();
        log(request, type, ids.toString(), null, exception.getMessage(), exception, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final String type, final List<String> ids) {
        if (ids != null && !ids.isEmpty()) {
            final List<List<String>> partitionedIds = Lists.partition(ids, 10000);
            partitionedIds.forEach(subIds -> hardDeleteDoc(type, subIds));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void softDelete(final String type, final String id, final MetacatRequestContext metacatRequestContext) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                final XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                builder.startObject().field(ElasticSearchDoc.Field.DELETED, true)
                    .field(ElasticSearchDoc.Field.TIMESTAMP, java.time.Instant.now().toEpochMilli())
                    .field(ElasticSearchDoc.Field.USER,
                    metacatRequestContext.getUserName()).endObject();
                client.prepareUpdate(esIndex, type, id)
                    .setRetryOnConflict(NO_OF_CONFLICT_RETRIES).setDoc(builder).get(esCallTimeout);
                ensureMigrationByCopy(type, Collections.singletonList(id));
                return null;
            });
        } catch (Exception e) {
            handleException("ElasticSearchUtil.softDelete", type, id, e,
                Metrics.CounterElasticSearchDelete.getMetricName());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void softDelete(final String type, final List<String> ids,
                           final MetacatRequestContext metacatRequestContext) {
        if (ids != null && !ids.isEmpty()) {
            final List<List<String>> partitionedIds = Lists.partition(ids, 100);
            partitionedIds.forEach(subIds -> softDeleteDoc(type, subIds, metacatRequestContext));
            partitionedIds.forEach(subIds -> ensureMigrationByCopy(type, subIds));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updates(final String type, final List<String> ids, final ObjectNode node) {
        if (ids != null && !ids.isEmpty()) {
            final List<List<String>> partitionedIds = Lists.partition(ids, 100);
            partitionedIds.forEach(subIds -> updateDocs(type, subIds, node));
            partitionedIds.forEach(subIds -> ensureMigrationByCopy(type, subIds));
        }
    }

    private void updateDocs(final String type, final List<String> ids, final ObjectNode node) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                final BulkRequestBuilder bulkRequest = client.prepareBulk();
                ids.forEach(id -> {
                    bulkRequest.add(client.prepareUpdate(esIndex, type, id)
                        .setRetryOnConflict(NO_OF_CONFLICT_RETRIES)
                        .setDoc(metacatJson.toJsonAsBytes(node), XContentType.JSON));
                });
                final BulkResponse bulkResponse = bulkRequest.execute().actionGet(esBulkCallTimeout);
                if (bulkResponse.hasFailures()) {
                    for (BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            handleException("ElasticSearchUtil.updateDocs.item", type, item.getId(),
                                item.getFailure().getCause(), Metrics.CounterElasticSearchUpdate.getMetricName());
                        }
                    }
                }
                return null;
            });
        } catch (Exception e) {
            handleException("ElasticSearchUtil.updatDocs", type, ids, e,
                Metrics.CounterElasticSearchBulkUpdate.getMetricName());
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void save(final String type, final String id, final ElasticSearchDoc doc) {
        saveToIndex(type, id, doc, esIndex);
        ensureMigrationByCopy(type, Collections.singletonList(id));
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void save(final String type, final List<ElasticSearchDoc> docs) {
        if (docs != null && !docs.isEmpty()) {
            final List<List<ElasticSearchDoc>> partitionedDocs = Lists.partition(docs, 100);
            partitionedDocs.forEach(subDocs -> bulkSaveToIndex(type, subDocs, esIndex));
            partitionedDocs.forEach(subDocs -> ensureMigrationBySave(type, subDocs));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toJsonString(final ElasticSearchDoc elasticSearchDoc) {
        final String result = metacatJson.toJsonString(toJsonObject(elasticSearchDoc));
        return result.replace("{}", "null");
    }

    private ObjectNode toJsonObject(final ElasticSearchDoc elasticSearchDoc) {
        final ObjectNode oMetadata = metacatJson.toJsonObject(elasticSearchDoc.getDto());
        //add the searchable definition metadata
        elasticSearchDoc.addSearchableDefinitionMetadata(oMetadata);
        //Adding the timestamp explicitly
        oMetadata.put(ElasticSearchDoc.Field.TIMESTAMP, elasticSearchDoc.getTimestamp());
        //True if this entity has been deleted
        oMetadata.put(ElasticSearchDoc.Field.DELETED, elasticSearchDoc.isDeleted());
        //True if this entity has been deleted
        oMetadata.put(ElasticSearchDoc.Field.USER, elasticSearchDoc.getUser());
        if (elasticSearchDoc.getRefreshMarker() != null) {
            oMetadata.put(ElasticSearchDoc.Field.REFRESH_MARKER, elasticSearchDoc.getRefreshMarker());
        }
        return oMetadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getTableIdsByUri(final String type, final String dataUri) {
        List<String> ids = Lists.newArrayList();
        // Run the query and get the response.
        if (dataUri != null) {
            final SearchRequestBuilder request = client.prepareSearch(esIndex)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("serde.uri", dataUri))
                .setSize(Integer.MAX_VALUE)
                .setFetchSource(false);
            final SearchResponse response = request.execute().actionGet(esCallTimeout);
            if (response.getHits().getHits().length != 0) {
                ids = getIds(response);
            }
        }
        return ids;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getTableIdsByCatalogs(final String type, final List<QualifiedName> qualifiedNames,
                                              final List<QualifiedName> excludeQualifiedNames) {
        List<String> ids = Lists.newArrayList();
        final QueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.termsQuery("name.qualifiedName.tree", qualifiedNames))
            .must(QueryBuilders.termQuery("deleted_", false))
            .mustNot(QueryBuilders.termsQuery("name.qualifiedName.tree", excludeQualifiedNames));

        // Run the query and get the response.
        final SearchRequestBuilder request = client.prepareSearch(esIndex)
            .setTypes(type)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(queryBuilder)
            .setSize(Integer.MAX_VALUE)  // TODO May break if too many tables returned back, change to Scroll
            .setFetchSource(false);
        final SearchResponse response = request.execute().actionGet(esCallTimeout);
        if (response.getHits().getHits().length != 0) {
            ids = getIds(response);
        }
        return ids;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getIdsByQualifiedName(final String type, final QualifiedName qualifiedName) {
        List<String> result = Lists.newArrayList();
        // Run the query and get the response.
        final QueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("name.qualifiedName.tree", qualifiedName))
            .must(QueryBuilders.termQuery("deleted_", false));
        final SearchRequestBuilder request = client.prepareSearch(esIndex)
            .setTypes(type)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(queryBuilder)
            .setSize(Integer.MAX_VALUE)
            .setFetchSource(false);
        final SearchResponse response = request.execute().actionGet(esCallTimeout);
        if (response.getHits().getHits().length != 0) {
            result = getIds(response);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> getQualifiedNamesByMarkerByNames(final String type,
                                                        final List<QualifiedName> qualifiedNames,
                                                        final Instant marker,
                                                        final List<QualifiedName> excludeQualifiedNames,
                                                        final Class<T> valueType) {
        final List<T> result = Lists.newArrayList();
        final List<String> names = qualifiedNames.stream().map(QualifiedName::toString).collect(Collectors.toList());
        final List<String> excludeNames = excludeQualifiedNames.stream().map(QualifiedName::toString)
            .collect(Collectors.toList());
        //
        // Run the query and get the response.
        final QueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.termsQuery("name.qualifiedName.tree", names))
            .must(QueryBuilders.termQuery("deleted_", false))
            .must(QueryBuilders.rangeQuery(ElasticSearchDoc.Field.TIMESTAMP).lte(marker.getMillis()))
            .mustNot(QueryBuilders.termsQuery("name.qualifiedName.tree", excludeNames))
            .mustNot(QueryBuilders.termQuery("refreshMarker_", marker.toString()));
        final SearchRequestBuilder request = client.prepareSearch(esIndex)
            .setTypes(type)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(queryBuilder)
            .setSize(Integer.MAX_VALUE);
        final SearchResponse response = request.execute().actionGet(esCallTimeout);
        if (response.getHits().getHits().length != 0) {
            result.addAll(parseResponse(response, valueType));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void refresh() {
        client.admin().indices().refresh(new RefreshRequest(esIndex)).actionGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElasticSearchDoc get(final String type, final String id) {
        return get(type, id, esIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElasticSearchDoc get(final String type, final String id, final String index) {
        ElasticSearchDoc result = null;
        final GetResponse response = client.prepareGet(index, type, id).execute().actionGet(esCallTimeout);
        if (response.isExists()) {
            result = parse(response);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final MetacatRequestContext metacatRequestContext, final String type,
                       final boolean softDelete) {
        SearchResponse response = client.prepareSearch(esIndex)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setScroll(new TimeValue(config.getElasticSearchScrollTimeout()))
            .setSize(config.getElasticSearchScrollFetchSize())
            .setQuery(QueryBuilders.termQuery("_type", type))
            .setFetchSource(false)
            .execute()
            .actionGet(esCallTimeout);
        while (true) {
            response = client.prepareSearchScroll(response.getScrollId())
                .setScroll(new TimeValue(config.getElasticSearchScrollTimeout())).execute().actionGet(esCallTimeout);
            //Break condition: No hits are returned
            if (response.getHits().getHits().length == 0) {
                break;
            }
            final List<String> ids = getIds(response);
            if (softDelete) {
                softDelete(type, ids, metacatRequestContext);
            } else {
                delete(type, ids);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void log(final String method, final String type, final String name, @Nullable final String data,
                    final String logMessage, @Nullable final Exception ex, final boolean error) {
        log(method, type, name, data, logMessage, ex, error, esIndex);
    }

    /**
     * Log the message in elastic search.
     *
     * @param method     method
     * @param type       type
     * @param name       name
     * @param data       data
     * @param logMessage message
     * @param ex         exception
     * @param error      is an error
     * @param index      es index
     */
    private void log(final String method, final String type, final String name, @Nullable final String data,
                     final String logMessage, @Nullable final Exception ex, final boolean error, final String index) {
        if (config.isElasticSearchPublishMetacatLogEnabled()) {
            try {
                final HashMap<String, Object> source = Maps.newHashMap();
                source.put("method", method);
                source.put("qname", name);
                source.put("type", type);
                source.put("data", data);
                source.put("error", error);
                source.put("message", logMessage);
                source.put("details", Throwables.getStackTraceAsString(ex));
                client.prepareIndex(index, "metacat-log").setSource(source).execute().actionGet(esCallTimeout);
            } catch (Exception e) {
                registry.counter(Metrics.CounterElasticSearchLog.getMetricName(),
                    Metrics.tagStatusFailureSet).increment();
                log.warn("Failed saving the log message in elastic search for index{} method {}, name {}. Message: {}",
                    index, method, name, e.getMessage());
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<TableDto> simpleSearch(final String searchString) {
        final List<TableDto> result = Lists.newArrayList();
        final SearchResponse response = client.prepareSearch(esIndex)
            .setTypes(ElasticSearchDoc.Type.table.name())
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(QueryBuilders.termQuery("_all", searchString))
            .setSize(Integer.MAX_VALUE)
            .execute()
            .actionGet(esCallTimeout);
        if (response.getHits().getHits().length != 0) {
            result.addAll(parseResponse(response, TableDto.class));
        }
        return result;
    }

    /**
     * Permanently delete index documents.
     *
     * @param type index type
     * @param ids  entity ids
     */
    private void hardDeleteDoc(final String type, final List<String> ids) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                final BulkRequestBuilder bulkRequest = client.prepareBulk();
                ids.forEach(id -> bulkRequest.add(client.prepareDelete(esIndex, type, id)));
                final BulkResponse bulkResponse = bulkRequest.execute().actionGet(esBulkCallTimeout);
                log.info("Deleting metadata of type {} with count {}", type, ids.size());
                if (bulkResponse.hasFailures()) {
                    for (BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            handleException("ElasticSearchUtil.bulkDelete.item", type, item.getId(),
                                item.getFailure().getCause(), Metrics.CounterElasticSearchDelete.getMetricName());
                        }
                    }
                }
                return null;
            });
        } catch (Exception e) {
            handleException("ElasticSearchUtil.bulkDelete", type, ids, e,
                Metrics.CounterElasticSearchBulkDelete.getMetricName());
        }
    }

    /**
     * Get class from elastic search doc type.
     *
     * @param type type in string
     * @return object class
     */
    private Class getClass(final String type) {
        return ElasticSearchDoc.Type.valueOf(type).getClazz();
    }

    private ElasticSearchDoc parse(final GetResponse response) {
        ElasticSearchDoc result = null;
        if (response.isExists()) {
            final Map<String, Object> responseMap = response.getSourceAsMap();
            final String user = (String) responseMap.get(ElasticSearchDoc.Field.USER);
            final boolean deleted = (boolean) responseMap.get(ElasticSearchDoc.Field.DELETED);
            final long timestamp = (long) responseMap.get(ElasticSearchDoc.Field.TIMESTAMP);
            @SuppressWarnings("unchecked") final Object dto = metacatJson.parseJsonValue(
                response.getSourceAsBytes(),
                getClass(response.getType())
            );
            result = new ElasticSearchDoc(response.getId(), dto, user, deleted, timestamp);
        }
        return result;
    }


    /*
     * Read the documents from source index then copy to merge index
     * @param type index type
     * @param ids list of doc ids
     */
    private void copyDocToMergeIndex(final String type, final List<String> ids) {
        final List<ElasticSearchDoc> docs = new ArrayList<>();
        ids.forEach(id -> {
            final ElasticSearchDoc doc = get(type, id);
            if (doc != null) {
                docs.add(doc);
            }
        });
        bulkSaveToIndex(type, docs, config.getMergeEsIndex());
    }

    /*
     * Check if in migration mode, copy to merge index
     * @param type index type
     * @param ids list of doc ids
     */
    private void ensureMigrationByCopy(final String type, final List<String> ids) {
        if (!Strings.isNullOrEmpty(config.getMergeEsIndex())) {
            copyDocToMergeIndex(type, ids);
        }
    }

    /*
     * Check if in migration mode, copy to merge index
     * @param type index type
     * @param ids list of doc ids
     */
    private void ensureMigrationBySave(final String type, final List<ElasticSearchDoc> docs) {
        if (!Strings.isNullOrEmpty(config.getMergeEsIndex())) {
            log.info("Bulk save to mergeEsIndex = {}", config.getMergeEsIndex());
            bulkSaveToIndex(type, docs, config.getMergeEsIndex());
        }
    }

    /* Use elasticSearch bulk API to mark the documents as deleted
    * @param type index type
    * @param ids list of entity ids
    * @param metacatRequestContext context containing the user name
    */
    private void softDeleteDoc(
        final String type,
        final List<String> ids,
        final MetacatRequestContext metacatRequestContext) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                final BulkRequestBuilder bulkRequest = client.prepareBulk();
                final XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                builder.startObject().field(ElasticSearchDoc.Field.DELETED, true)
                    .field(ElasticSearchDoc.Field.TIMESTAMP, java.time.Instant.now().toEpochMilli())
                    .field(ElasticSearchDoc.Field.USER, metacatRequestContext.getUserName()).endObject();
                ids.forEach(id -> bulkRequest.add(client.prepareUpdate(esIndex, type, id)
                    .setRetryOnConflict(NO_OF_CONFLICT_RETRIES).setDoc(builder)));
                final BulkResponse bulkResponse = bulkRequest.execute().actionGet(esBulkCallTimeout);
                if (bulkResponse.hasFailures()) {
                    for (BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            handleException("ElasticSearchUtil.bulkSoftDelete.item", type, item.getId(),
                                item.getFailure().getCause(), Metrics.CounterElasticSearchDelete.getMetricName());
                        }
                    }
                }
                return null;
            });
        } catch (Exception e) {
            handleException("ElasticSearchUtil.bulkSoftDelete", type, ids, e,
                Metrics.CounterElasticSearchBulkDelete.getMetricName());
        }
    }

    /**
     * Save of a single entity to an index.
     *
     * @param type  index type
     * @param id    id of the entity
     * @param doc   source string of the entity
     * @param index the index name
     */
    private void saveToIndex(final String type, final String id, final ElasticSearchDoc doc, final String index) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                final IndexRequestBuilder indexRequestBuilder = prepareIndexRequest(index, type, doc);
                if (indexRequestBuilder != null) {
                    indexRequestBuilder.execute().actionGet(esCallTimeout);
                }
                return null;
            });
        } catch (Exception e) {
            handleException("ElasticSearchUtil.saveToIndex", type, id, e,
                Metrics.CounterElasticSearchSave.getMetricName());
        }
    }

    private static List<String> getIds(final SearchResponse response) {
        final List<String> ret = Lists.newArrayList();
        for (SearchHit hit : response.getHits().getHits()) {
            ret.add(hit.getId());
        }
        return ret;
    }

    private <T> List<T> parseResponse(final SearchResponse response, final Class<T> valueType) {
        final List<T> ret = Lists.newArrayList();
        for (SearchHit hit : response.getHits().getHits()) {
            try {
                ret.add(metacatJson.parseJsonValue(hit.getSourceAsString(), valueType));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        return ret;
    }

    /**
     * Bulk save of the entities.
     *
     * @param type index type
     * @param docs metacat documents
     */
    private void bulkSaveToIndex(final String type, final List<ElasticSearchDoc> docs, final String index) {
        if (docs != null && !docs.isEmpty()) {
            try {

                RETRY_ES_PUBLISH.call(() -> {
                    final BulkRequestBuilder bulkRequest = client.prepareBulk();
                    for (ElasticSearchDoc doc : docs) {
                        final IndexRequestBuilder indexRequestBuilder = prepareIndexRequest(index, type, doc);
                        if (indexRequestBuilder != null) {
                            bulkRequest.add(indexRequestBuilder);
                        }
                    }

                    if (bulkRequest.numberOfActions() > 0) {
                        final BulkResponse bulkResponse = bulkRequest.execute().actionGet(esBulkCallTimeout);
                        log.info("Bulk saving metadata of index {} type {} with size {}.",
                            index, type, docs.size());
                        if (bulkResponse.hasFailures()) {
                            for (BulkItemResponse item : bulkResponse.getItems()) {
                                if (item.isFailed()) {
                                    handleException("ElasticSearchUtil.bulkSaveToIndex.index", type, item.getId(),
                                        item.getFailure().getCause(), Metrics.CounterElasticSearchSave.getMetricName());
                                }
                            }
                        }
                    }
                    return null;
                });
            } catch (Exception e) {
                final List<String> docIds = docs.stream().map(ElasticSearchDoc::getId).collect(Collectors.toList());
                handleException("ElasticSearchUtil.bulkSaveToIndex", type, docIds, e,
                    Metrics.CounterElasticSearchBulkSave.getMetricName());
            }
        }
    }

    IndexRequestBuilder prepareIndexRequest(final String index,
                                            final String type,
                                            final ElasticSearchDoc doc) {
        return client.prepareIndex(index, type, doc.getId()).setSource(toJsonString(doc), XContentType.JSON);
    }

}
