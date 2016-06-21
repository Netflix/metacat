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
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.monitoring.CounterWrapper;
import com.netflix.metacat.common.server.Config;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.netflix.metacat.main.services.search.ElasticSearchDoc.Field.DELETED;
import static com.netflix.metacat.main.services.search.ElasticSearchDoc.Field.USER;
import static com.netflix.metacat.main.services.search.ElasticSearchDoc.Type.table;

/**
 * Created by amajumdar on 8/12/15.
 */
public class ElasticSearchUtil {
    private XContentType contentType = Requests.INDEX_CONTENT_TYPE;
    private final String esIndex;
    private static final Retryer<Void> RETRY_ES_PUBLISH = RetryerBuilder.<Void>newBuilder()
            .retryIfExceptionOfType(ElasticsearchException.class)
            .withWaitStrategy(WaitStrategies.incrementingWait(10, TimeUnit.SECONDS, 30, TimeUnit.SECONDS))
            .withStopStrategy(StopStrategies.stopAfterAttempt(3))
            .build();
    private final Client client;
    private final Config config;
    private final MetacatJson metacatJson;
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchUtil.class);

    @Inject
    public ElasticSearchUtil(@Nullable Client client, Config config, MetacatJson metacatJson) {
        this.config = config;
        this.client = client;
        this.metacatJson = metacatJson;
        this.esIndex = config.getEsIndex();
    }

    /**
     * Delete index document
     * @param type index type
     * @param id entity id
     */
    public void delete(String type, String id) {
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
     * Delete index documents
     * @param type index type
     * @param ids entity ids
     */
    public void delete(String type, List<String> ids) {
        if(ids == null || ids.isEmpty()){
            return;
        }
        try {
            RETRY_ES_PUBLISH.call(() -> {
                BulkRequestBuilder bulkRequest = client.prepareBulk();
                ids.forEach(id -> bulkRequest.add( client.prepareDelete(esIndex, type, id)));
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if(bulkResponse.hasFailures()){
                    for(BulkItemResponse item: bulkResponse.getItems()){
                        if( item.isFailed()){
                            log.error("Failed deleting metadata of type {} with id {}. Message: {}", type, item.getId(), item.getFailureMessage());
                            CounterWrapper.incrementCounter("dse.metacat.esDeleteFailure");
                            log("ElasticSearchUtil.bulkDelete", type, item.getId(), null, item.getFailureMessage(), null, true);
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
     * Marks the document as deleted
     * @param type index type
     * @param id entity id
     * @param metacatContext context containing the user name
     */
    public void softDelete(String type, String id, MetacatContext metacatContext) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                builder.startObject().field(DELETED, true).field(USER,
                        metacatContext.getUserName()).endObject();
                client.prepareUpdate(esIndex, type, id).setDoc(builder).get();
                return null;
            });
        } catch (Exception e) {
            log.error(String.format("Failed deleting metadata of type %s with id %s", type, id), e);
            CounterWrapper.incrementCounter("dse.metacat.esDeleteFailure");
            log("ElasticSearchUtil.softDelete", type, id, null, e.getMessage(), e, true);
        }
    }

    /**
     * Marks the documents as deleted
     * @param type index type
     * @param ids list of entity ids
     * @param metacatContext context containing the user name
     */
    public void softDelete(String type, List<String> ids, MetacatContext metacatContext) {
        if( ids != null && !ids.isEmpty()) {
            List<List<String>> partitionedDocs = Lists.partition(ids, 100);
            partitionedDocs.forEach(subIds -> _softDelete( type, subIds, metacatContext));
        }
    }

    private void _softDelete(String type, List<String> ids, MetacatContext metacatContext) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                BulkRequestBuilder bulkRequest = client.prepareBulk();
                XContentBuilder builder = XContentFactory.contentBuilder(contentType);
                builder.startObject().field(DELETED, true).field(USER,
                        metacatContext.getUserName()).endObject();
                ids.forEach(id -> bulkRequest.add( client.prepareUpdate(esIndex, type, id).setDoc(builder)));
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    for (BulkItemResponse item : bulkResponse.getItems()) {
                        if (item.isFailed()) {
                            log.error("Failed soft deleting metadata of type {} with id {}. Message: {}", type, item.getId(),
                                    item.getFailureMessage());
                            CounterWrapper.incrementCounter("dse.metacat.esDeleteFailure");
                            log("ElasticSearchUtil.bulkSoftDelete", type, item.getId(), null, item.getFailureMessage(), null, true);
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
     * Updates the documents with partial updates with the given fields
     * @param type index type
     * @param ids list of entity ids
     * @param metacatContext context containing the user name
     * @param node json that represents the document source
     */
    public void updates(String type, List<String> ids, MetacatContext metacatContext, ObjectNode node) {
        if(ids == null || ids.isEmpty()){
            return;
        }
        try {
            RETRY_ES_PUBLISH.call(() -> {
                BulkRequestBuilder bulkRequest = client.prepareBulk();
                ids.forEach(id -> {
                    node.put(USER, metacatContext.getUserName());
                    bulkRequest.add( client.prepareUpdate(esIndex, type, id).setDoc(metacatJson.toJsonAsBytes(node)));
                });
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
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
     * Save of a single entity
     * @param type index type
     * @param id id of the entity
     * @param body source string of the entity
     */
    public void save(String type, String id, String body) {
        try {
            RETRY_ES_PUBLISH.call(() -> {
                client.prepareIndex(esIndex, type, id).setSource(body).execute().actionGet();
                return null;
            });
        } catch (Exception e) {
            log.error(String.format("Failed saving metadata of type %s with id %s", type, id), e);
            CounterWrapper.incrementCounter("dse.metacat.esSaveFailure");
            log("ElasticSearchUtil.save", type, id, null, e.getMessage(), e, true);
        }
    }

    /**
     * Bulk save of the entities
     * @param type index type
     * @param docs metacat documents
     */
    public void save(String type, List<ElasticSearchDoc> docs) {
        if( docs != null && !docs.isEmpty()) {
            List<List<ElasticSearchDoc>> partitionedDocs = Lists.partition(docs, 100);
            partitionedDocs.forEach(subDocs -> _save(type, subDocs));
        }
    }

    /**
     * Bulk save of the entities
     * @param type index type
     * @param docs metacat documents
     */
    private void _save(String type, List<ElasticSearchDoc> docs) {
        if( docs != null && !docs.isEmpty()) {
            try {
                RETRY_ES_PUBLISH.call(() -> {
                    BulkRequestBuilder bulkRequest = client.prepareBulk();
                    docs.forEach(doc -> bulkRequest.add(client.prepareIndex(esIndex, type, doc.getId())
                            .setSource(doc.toJsonString())));
                    if (bulkRequest.numberOfActions() > 0) {
                        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                        if (bulkResponse.hasFailures()) {
                            for (BulkItemResponse item : bulkResponse.getItems()) {
                                if (item.isFailed()) {
                                    log.error("Failed saving metadata of type {} with id {}. Message: {}", type, item.getId(),
                                            item.getFailureMessage());
                                    CounterWrapper.incrementCounter("dse.metacat.esSaveFailure");
                                    log("ElasticSearchUtil.bulkSave", type, item.getId(), null,
                                            item.getFailureMessage(), null, true);
                                }
                            }
                        }
                    }
                    return null;
                });
            } catch (Exception e) {
                log.error(String.format("Failed saving metadatas of type %s", type), e);
                CounterWrapper.incrementCounter("dse.metacat.esBulkSaveFailure");
                List<String> docIds = docs.stream().map(ElasticSearchDoc::getId).collect(Collectors.toList());
                log("ElasticSearchUtil.bulkSave", type, docIds.toString(), null, e.getMessage(), e, true);
            }
        }
    }

    public String toJsonString(String id, Object dto, MetacatContext context, boolean isDeleted){
        return new ElasticSearchDoc( id, dto, context.getUserName(), isDeleted).toJsonString();
    }


    public List<String> getTableIdsByUri(String type, String dataUri) {
        List<String> ids = Lists.newArrayList();
        if( dataUri != null) {
            //
            // Run the query and get the response.
            SearchRequestBuilder request = client.prepareSearch(esIndex)
                    .setTypes(type)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.termQuery("serde.uri", dataUri))
                    .setSize(Integer.MAX_VALUE)
                    .setNoFields();
            SearchResponse response = request.execute().actionGet();
            if (response.getHits().hits().length != 0) {
                ids =  getIds(response);
            }
        }
        return ids;
    }

    public List<String> getTableIdsByCatalogs(String type, List<QualifiedName> qualifiedNames) {
        List<String> ids = Lists.newArrayList();
        //
        // Run the query and get the response.
        SearchRequestBuilder request = client.prepareSearch(esIndex)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termsQuery("name.qualifiedName.tree", qualifiedNames))
                .setSize(Integer.MAX_VALUE)
                .setNoFields();
        SearchResponse response = request.execute().actionGet();
        if (response.getHits().hits().length != 0) {
            ids =  getIds(response);
        }
        return ids;
    }

    public <T> List<T> getQualifiedNamesByMarkerByNames(String type, List<QualifiedName> qualifiedNames, String marker, Class<T> valueType) {
        List<T> result = Lists.newArrayList();
        List<String> names = qualifiedNames.stream().map(QualifiedName::toString).collect(Collectors.toList());
        //
        // Run the query and get the response.
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("name.qualifiedName.tree", names))
                .must(QueryBuilders.termQuery("deleted_", false))
                .mustNot(QueryBuilders.termQuery("refreshMarker_", marker));
        SearchRequestBuilder request = client.prepareSearch(esIndex)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                .setSize(Integer.MAX_VALUE);
        SearchResponse response = request.execute().actionGet();
        if (response.getHits().hits().length != 0) {
            result.addAll( parseResponse(response, valueType));
        }
        return result;
    }

    private static List<String> getIds(final SearchResponse response) {
        return FluentIterable.from(response.getHits()).transform(SearchHit::getId).toList();
    }

    private <T> List<T> parseResponse(final SearchResponse response, final Class<T> valueType) {
        return FluentIterable.from(response.getHits()).transform(hit -> {
            try {
                return metacatJson.parseJsonValue(hit.getSourceAsString(), valueType);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }).toList();
    }

    public void refresh(){
        client.admin().indices().refresh(new RefreshRequest(esIndex)).actionGet();
    }

    public  ElasticSearchDoc get(String type, String id) {
        ElasticSearchDoc result = null;
        GetResponse response = client.prepareGet(esIndex, type, id).execute().actionGet();
        if( response.isExists()){
            result = ElasticSearchDoc.parse(response);
        }
        return result;
    }

    public  void delete(MetacatContext metacatContext, String type, boolean softDelete) {
        SearchResponse response = client.prepareSearch(esIndex)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(config.getElasticSearchScrollTimeout()))
                .setSize(config.getElasticSearchScrollFetchSize())
                .setQuery(QueryBuilders.termQuery("_type", type))
                .setNoFields()
                .execute()
                .actionGet();
        while(true){
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(config.getElasticSearchScrollTimeout())).execute().actionGet();
            //Break condition: No hits are returned
            if (response.getHits().getHits().length == 0) {
                break;
            }
            List<String> ids = getIds(response);
            if( softDelete){
                softDelete( type, ids, metacatContext);
            } else {
                delete( type, ids);
            }
        }
    }

    public void log(String method, String type, String name, String data, String logMessage, Exception ex, boolean error){
        try {
            Map<String, Object> source = Maps.newHashMap();
            source.put("method", method);
            source.put("name", name);
            source.put("type", type);
            source.put("data", data);
            source.put("error", error);
            source.put("message", logMessage);
            source.put("details", Throwables.getStackTraceAsString(ex));
            client.prepareIndex(esIndex, "metacat-log").setSource(source).execute().actionGet();
        } catch(Exception e){
           log.warn("Failed saving the log message in elastic search for method {}, name {}. Message: {}", method, name, e.getMessage());
        }
    }

    public List<TableDto> simpleSearch(String searchString){
        List<TableDto> result = Lists.newArrayList();
        SearchResponse response = client.prepareSearch(esIndex)
                .setTypes(table.name())
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("_all", searchString))
                .setSize(Integer.MAX_VALUE)
                .execute()
                .actionGet();
        if (response.getHits().hits().length != 0) {
            result.addAll( parseResponse(response, TableDto.class));
        }
        return result;
    }
}
