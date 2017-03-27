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
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.monitoring.CounterWrapper;
import com.netflix.metacat.common.server.Config;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Utility class for index, update, delete metacat doc from elastic search.
 */
@Slf4j
public class ElasticSearchUtilImpl implements ElasticSearchUtil {
    protected static final Retryer<Void> RETRY_ES_PUBLISH = RetryerBuilder.<Void>newBuilder()
        .retryIfExceptionOfType(ElasticsearchException.class)
        .withWaitStrategy(WaitStrategies.incrementingWait(10, TimeUnit.SECONDS, 30, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
        .build();
    protected XContentType contentType = Requests.INDEX_CONTENT_TYPE;
    protected final String esIndex;
    protected final Client client;
    private final Config config;
    private final MetacatJson metacatJson;

    /**
     * Constructor.
     * @param client elastic search client
     * @param config config
     * @param metacatJson json utility
     */
    @Inject
    public ElasticSearchUtilImpl(
        @Nullable
        final Client client, final Config config, final MetacatJson metacatJson) {
        this.config = config;
        this.client = client;
        this.metacatJson = metacatJson;
        this.esIndex = config.getEsIndex();
    }

    /**
     * Delete index document.
     * @param type index type
     * @param id entity id
     */
    public void delete(final String type, final String id) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                client.prepareDelete(esIndex, type, id).execute().actionGet();
                return null;
            });
        } catch (Exception e) {
            log.error(String.format("Failed deleting metadata of type %s with id %s.", type, id), e);
            CounterWrapper.incrementCounter("dse.metacat.esDeleteFailure");
            log("ElasticSearchUtil.delete", type, id, null, e.getMessage(), e, true);
        }
    }

    /**
     * Delete index documents.
     * @param type index type
     * @param ids entity ids
     */
    public void delete(final String type, final List<String> ids) {
        if (ids != null && !ids.isEmpty()) {
            final List<List<String>> partitionedIds = Lists.partition(ids, 10000);
            partitionedIds.forEach(subIds -> hardDeleteDoc(type, subIds));
        }
    }

    /**
     * Permanently delete index documents.
     * @param type index type
     * @param ids entity ids
     */
    private void hardDeleteDoc(final String type, final List<String> ids) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                final BulkRequestBuilder bulkRequest = client.prepareBulk();
                ids.forEach(id -> bulkRequest.add(client.prepareDelete(esIndex, type, id)));
                final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                log.info("Deleting metadata of type {} with count {}", type, ids.size());
                if (bulkResponse.hasFailures()) {
                    for (BulkItemResponse item: bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            log.error("Failed deleting metadata of type {} with id {}. Message: {}",
                                type, item.getId(), item.getFailureMessage());
                            CounterWrapper.incrementCounter("dse.metacat.esDeleteFailure");
                            log("ElasticSearchUtil.bulkDelete", type, item.getId(), null, item.getFailureMessage(),
                                null, true);
                        }
                    }
                }
                return null;
            });
        } catch (Exception e) {
            log.error(String.format("Failed deleting metadata of type %s with ids %s", type, ids), e);
            CounterWrapper.incrementCounter("dse.metacat.esBulkDeleteFailure");
            log("ElasticSearchUtil.bulkDelete", type, ids.toString(), null, e.getMessage(), e, true);
        }
    }

    /**
     * Marks the document as deleted.
     * @param type index type
     * @param id entity id
     * @param metacatRequestContext context containing the user name
     */
    public void softDelete(final String type, final String id, final MetacatRequestContext metacatRequestContext) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                final XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                builder.startObject().field(ElasticSearchDoc.Field.DELETED, true).field(ElasticSearchDoc.Field.USER,
                    metacatRequestContext.getUserName()).endObject();
                client.prepareUpdate(esIndex, type, id).setDoc(builder).get();
                ensureMigrationByCopy(type, Collections.singletonList(id));
                return null;
            });
        } catch (Exception e) {
            log.error(String.format("Failed deleting metadata of type %s with id %s", type, id), e);
            CounterWrapper.incrementCounter("dse.metacat.esDeleteFailure");
            log("ElasticSearchUtil.softDelete", type, id, null, e.getMessage(), e, true);
        }
    }

    /**
     * Batch marks the documents as deleted.
     * @param type index type
     * @param ids list of entity ids
     * @param metacatRequestContext context containing the user name
     */
    public void softDelete(final String type, final List<String> ids,
                           final MetacatRequestContext metacatRequestContext) {
        if (ids != null && !ids.isEmpty()) {
            final List<List<String>> partitionedIds = Lists.partition(ids, 100);
            partitionedIds.forEach(subIds -> softDeleteDoc(type, subIds, metacatRequestContext));
            partitionedIds.forEach(subIds -> ensureMigrationByCopy(type, subIds));
        }
    }

    /* Use elasticSearch bulk API to mark the documents as deleted
     * @param type index type
     * @param ids list of entity ids
     * @param metacatRequestContext context containing the user name
     */
    void softDeleteDoc(final String type, final List<String> ids, final MetacatRequestContext metacatRequestContext) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                final BulkRequestBuilder bulkRequest = client.prepareBulk();
                final XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                builder.startObject().field(ElasticSearchDoc.Field.DELETED, true).field(ElasticSearchDoc.Field.USER,
                    metacatRequestContext.getUserName()).endObject();
                ids.forEach(id -> bulkRequest.add(client.prepareUpdate(esIndex, type, id).setDoc(builder)));
                final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    for (BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            log.error("Failed soft deleting metadata of type {} with id {}. Message: {}",
                                type, item.getId(), item.getFailureMessage());
                            CounterWrapper.incrementCounter("dse.metacat.esDeleteFailure");
                            log("ElasticSearchUtil.bulkSoftDelete",
                                type, item.getId(), null, item.getFailureMessage(), null, true);
                        }
                    }
                }
                return null;
            });
        } catch (Exception e) {
            log.error(String.format("Failed soft deleting metadata of type %s with ids %s", type, ids), e);
            CounterWrapper.incrementCounter("dse.metacat.esBulkDeleteFailure");
            log("ElasticSearchUtil.bulkSoftDelete", type, ids.toString(), null, e.getMessage(), e, true);
        }
    }

    /**
     * Batch updates the documents with partial updates with the given fields.
     * @param type index type
     * @param ids list of entity ids
     * @param metacatRequestContext context containing the user name
     * @param node json that represents the document source
     */
    public void updates(final String type, final List<String> ids,
                        final MetacatRequestContext metacatRequestContext, final ObjectNode node) {

        if (ids != null && !ids.isEmpty()) {
            final List<List<String>> partitionedIds = Lists.partition(ids, 100);
            partitionedIds.forEach(subIds -> updateDocs(type, subIds, metacatRequestContext, node));
            partitionedIds.forEach(subIds -> ensureMigrationByCopy(type, subIds));
        }
    }

    /**
     * Updates the documents with partial updates with the given fields.
     * @param type index type
     * @param ids list of entity ids
     * @param metacatRequestContext context containing the user name
     * @param node json that represents the document source
     */
    private void updateDocs(final String type, final List<String> ids,
                            final MetacatRequestContext metacatRequestContext, final ObjectNode node) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                final BulkRequestBuilder bulkRequest = client.prepareBulk();
                ids.forEach(id -> {
                    node.put(ElasticSearchDoc.Field.USER, metacatRequestContext.getUserName());
                    bulkRequest.add(client.prepareUpdate(esIndex, type, id).setDoc(metacatJson.toJsonAsBytes(node)));
                });
                final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    for (BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            log.error("Failed updating metadata of type {} with id {}. Message: {}", type, item.getId(),
                                item.getFailureMessage());
                            CounterWrapper.incrementCounter("dse.metacat.esUpdateFailure");
                        }
                    }
                }
                return null;
            });
        } catch (Exception e) {
            log.error(String.format("Failed updating metadata of type %s with ids %s", type, ids), e);
            CounterWrapper.incrementCounter("dse.metacat.esBulkUpdateFailure");
            log("ElasticSearchUtil.updates", type, ids.toString(), null, e.getMessage(), e, true);
        }
    }
    /**
     * Save of a single entity.
     * @param type index type
     * @param id id of the entity
     * @param body source string of the entity
     */
    public void save(final String type, final String id, final String body) {
        saveToIndex(type, id, body, esIndex);
        ensureMigrationByCopy(type, Collections.singletonList(id));
    }

    /**
     * Save of a single entity to an index.
     * @param type index type
     * @param id id of the entity
     * @param body source string of the entity
     * @param index the index name
     */
    void saveToIndex(final String type, final String id, final String body, final String index) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                client.prepareIndex(index, type, id).setSource(body).execute().actionGet();
                return null;
            });
        } catch (Exception e) {
            log.error(String.format("Failed saving metadata of index %s type %s with id %s", index, type, id), e);
            CounterWrapper.incrementCounter("dse.metacat.esSaveFailure");
            log("ElasticSearchUtil.save", type, id, null, e.getMessage(), e, true, index);
        }
    }

    /**
     * Bulk save of the entities.
     * @param type index type
     * @param docs metacat documents
     */
    public void save(final String type, final List<ElasticSearchDoc> docs) {
        if (docs != null && !docs.isEmpty()) {
            final List<List<ElasticSearchDoc>> partitionedDocs = Lists.partition(docs, 100);
            partitionedDocs.forEach(subDocs -> bulkSaveToIndex(type, subDocs, esIndex));
            partitionedDocs.forEach(subDocs -> ensureMigrationBySave(type, subDocs));
        }
    }

    /**
     * Bulk save of the entities.
     * @param type index type
     * @param docs metacat documents
     */
    void bulkSaveToIndex(final String type, final List<ElasticSearchDoc> docs, final String index) {
        if (docs != null && !docs.isEmpty()) {
            try {
                RETRY_ES_PUBLISH.call(() -> {
                    final BulkRequestBuilder bulkRequest = client.prepareBulk();
                    docs.forEach(doc -> bulkRequest.add(client.prepareIndex(index, type, doc.getId())
                        .setSource(doc.toJsonString())));
                    if (bulkRequest.numberOfActions() > 0) {
                        final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                        log.info("Bulk saving metadata of index {} type {} with size {}.",
                            index, type, docs.size());
                        if (bulkResponse.hasFailures()) {
                            for (BulkItemResponse item : bulkResponse.getItems()) {
                                if (item.isFailed()) {
                                    log.error("Failed saving metadata of {} index type {} with id {}. Message: {}",
                                        index, type, item.getId(), item.getFailureMessage());
                                    CounterWrapper.incrementCounter("dse.metacat.esSaveFailure");
                                    log("ElasticSearchUtil.bulkSave", type, item.getId(), null,
                                        item.getFailureMessage(), null, true, index);
                                }
                            }
                        }
                    }
                    return null;
                });
            } catch (Exception e) {
                log.error(String.format("Failed saving metadatas of index %s type %s", index, type), e);
                CounterWrapper.incrementCounter("dse.metacat.esBulkSaveFailure");
                final List<String> docIds = docs.stream().map(ElasticSearchDoc::getId).collect(Collectors.toList());
                log("ElasticSearchUtil.bulkSave", type, docIds.toString(), null, e.getMessage(), e, true, index);
            }
        }
    }

    /**
     * Creates JSON from search doc.
     * @param id doc id
     * @param dto dto
     * @param context context
     * @param isDeleted true if it has to be mark deleted
     * @return doc
     */
    public String toJsonString(final String id, final Object dto, final MetacatRequestContext context,
                               final boolean isDeleted) {
        return new ElasticSearchDoc(id, dto, context.getUserName(), isDeleted).toJsonString();
    }

    /**
     * List table names by uri.
     * @param type doc type
     * @param dataUri uri
     * @return list of table names
     */
    public List<String> getTableIdsByUri(final String type, final String dataUri) {
        List<String> ids = Lists.newArrayList();
        if (dataUri != null) {
            //
            // Run the query and get the response.
            final SearchRequestBuilder request = client.prepareSearch(esIndex)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("serde.uri", dataUri))
                .setSize(Integer.MAX_VALUE)
                .setNoFields();
            final SearchResponse response = request.execute().actionGet();
            if (response.getHits().hits().length != 0) {
                ids = getIds(response);
            }
        }
        return ids;
    }

    /**
     * List table names.
     * @param type doc type
     * @param qualifiedNames names
     * @param excludeQualifiedNames exclude names
     * @return list of table names
     */
    public List<String> getTableIdsByCatalogs(final String type, final List<QualifiedName> qualifiedNames,
                                              final List<QualifiedName> excludeQualifiedNames) {
        List<String> ids = Lists.newArrayList();
        final QueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.termsQuery("name.qualifiedName.tree", qualifiedNames))
            .must(QueryBuilders.termQuery("deleted_", false))
            .mustNot(QueryBuilders.termsQuery("name.qualifiedName.tree", excludeQualifiedNames));
        //
        // Run the query and get the response.
        final SearchRequestBuilder request = client.prepareSearch(esIndex)
            .setTypes(type)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(queryBuilder)
            .setSize(Integer.MAX_VALUE)
            .setNoFields();
        final SearchResponse response = request.execute().actionGet();
        if (response.getHits().hits().length != 0) {
            ids = getIds(response);
        }
        return ids;
    }

    /**
     * List of names.
     * @param type type
     * @param qualifiedName name
     * @return list of names
     */
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
            .setNoFields();
        final SearchResponse response = request.execute().actionGet();
        if (response.getHits().hits().length != 0) {
            result = getIds(response);
        }
        return result;
    }

    /**
     * Search the names by names and by the given marker.
     * @param type type
     * @param qualifiedNames names
     * @param marker marker
     * @param excludeQualifiedNames exclude names
     * @param valueType dto type
     * @param <T> dto type
     * @return dto
     */
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
            .must(QueryBuilders.rangeQuery("_timestamp").lte(marker.toDate()))
            .mustNot(QueryBuilders.termsQuery("name.qualifiedName.tree", excludeNames))
            .mustNot(QueryBuilders.termQuery("refreshMarker_", marker.toString()));
        final SearchRequestBuilder request = client.prepareSearch(esIndex)
            .setTypes(type)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(queryBuilder)
            .setSize(Integer.MAX_VALUE);
        final SearchResponse response = request.execute().actionGet();
        if (response.getHits().hits().length != 0) {
            result.addAll(parseResponse(response, valueType));
        }
        return result;
    }

    protected static List<String> getIds(final SearchResponse response) {
        return FluentIterable.from(response.getHits()).transform(SearchHit::getId).toList();
    }

    protected  <T> List<T> parseResponse(final SearchResponse response, final Class<T> valueType) {
        return FluentIterable.from(response.getHits()).transform(hit -> {
            try {
                return metacatJson.parseJsonValue(hit.getSourceAsString(), valueType);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }).toList();
    }

    /**
     * Elastic search index refresh.
     */
    public void refresh() {
        client.admin().indices().refresh(new RefreshRequest(esIndex)).actionGet();
    }

    /**
     * Gets the document for the given type and id.
     * @param type doc type
     * @param id doc id
     * @return doc
     */
    public ElasticSearchDoc get(final String type, final String id) {
        return get(type, id, esIndex);
    }

    /**
     * Gets the document for the given type and id.
     * @param type doc type
     * @param id doc id
     * @param index the es index
     * @return doc
     */
    public ElasticSearchDoc get(final String type, final String id, final String index) {
        ElasticSearchDoc result = null;
        final GetResponse response = client.prepareGet(index, type, id).execute().actionGet();
        if (response.isExists()) {
            result = ElasticSearchDoc.parse(response);
        }
        return result;
    }

    /**
     * Delete the records for the given type.
     * @param metacatRequestContext context
     * @param type doc type
     * @param softDelete if true, marks the doc for deletion
     */
    public void delete(final MetacatRequestContext metacatRequestContext, final String type,
                       final boolean softDelete) {
        SearchResponse response = client.prepareSearch(esIndex)
            .setSearchType(SearchType.SCAN)
            .setScroll(new TimeValue(config.getElasticSearchScrollTimeout()))
            .setSize(config.getElasticSearchScrollFetchSize())
            .setQuery(QueryBuilders.termQuery("_type", type))
            .setNoFields()
            .execute()
            .actionGet();
        while (true) {
            response = client.prepareSearchScroll(response.getScrollId())
                .setScroll(new TimeValue(config.getElasticSearchScrollTimeout())).execute().actionGet();
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
     * Wrapper for logging the message in elastic search esIndex.
     * @param method method
     * @param type type
     * @param name name
     * @param data data
     * @param logMessage message
     * @param ex exception
     * @param error is an error
     */
    public void log(final String method, final String type, final String name, final String data,
                    final String logMessage, final Exception ex, final boolean error) {
        log(method, type, name, data, logMessage, ex, error, esIndex);
    }

    /**
     * Log the message in elastic search.
     * @param method method
     * @param type type
     * @param name name
     * @param data data
     * @param logMessage message
     * @param ex exception
     * @param error is an error
     * @param index es index
     */
    void log(final String method, final String type, final String name, final String data,
             final String logMessage, final Exception ex, final boolean error, final String index) {
        try {
            final Map<String, Object> source = Maps.newHashMap();
            source.put("method", method);
            source.put("name", name);
            source.put("type", type);
            source.put("data", data);
            source.put("error", error);
            source.put("message", logMessage);
            source.put("details", Throwables.getStackTraceAsString(ex));
            client.prepareIndex(index, "metacat-log").setSource(source).execute().actionGet();
        } catch (Exception e) {
            log.warn("Failed saving the log message in elastic search for index{} method {}, name {}. Message: {}",
                index, method, name, e.getMessage());
        }
    }

    /**
     * Full text search.
     * @param searchString search text
     * @return list of table info
     */
    public List<TableDto> simpleSearch(final String searchString) {
        final List<TableDto> result = Lists.newArrayList();
        final SearchResponse response = client.prepareSearch(esIndex)
            .setTypes(ElasticSearchDoc.Type.table.name())
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setQuery(QueryBuilders.termQuery("_all", searchString))
            .setSize(Integer.MAX_VALUE)
            .execute()
            .actionGet();
        if (response.getHits().hits().length != 0) {
            result.addAll(parseResponse(response, TableDto.class));
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
            log.info("Bulk save to mergeEsIndex = " + config.getMergeEsIndex());
            bulkSaveToIndex(type, docs, config.getMergeEsIndex());
        }
    }
}
