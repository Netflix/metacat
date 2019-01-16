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


package com.netflix.metacat.main.services.search

import com.fasterxml.jackson.databind.node.ObjectNode
import com.netflix.metacat.common.MetacatRequestContext
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.testdata.provider.DataDtoProvider
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.elasticsearch.action.ListenableActionFuture
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequestBuilder
import org.elasticsearch.action.get.GetRequestBuilder
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.action.update.UpdateRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.client.Response
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits
import org.elasticsearch.transport.TransportException
import org.joda.time.Instant
import spock.lang.Shared
import spock.lang.Specification

import static com.netflix.metacat.main.services.search.ElasticSearchDoc.Type

/**
 * Testing suit for elastic search util
 */
class ElasticSearchUtilSpec extends Specification {
    @Shared
    Config config = Mock(Config)
    @Shared
    MetacatJson metacatJson = new MetacatJsonLocator()
    @Shared
    String esIndex = "metacat"
    @Shared
    def registry = new SimpleMeterRegistry()

    def setupSpec() {
        config.isElasticSearchPublishMetacatLogEnabled() >> true
        config.getEsIndex() >> esIndex
    }

    def "Test save for #id"() {
        def catalogName = 'prodhive'
        def databaseName = 'estestdb'
        def tableName = 'part'
        def id = 'prodhive/amajumdar/part_test'
        def client = Mock(Client)
        def indexRequestBuilder = Mock(IndexRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b")
        def ElasticSearchDoc doc = new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)

        when:
            es.save(Type.table.name(), id, doc)

        then:
        1 * client.prepareIndex(_, _, _) >> indexRequestBuilder
        1 * indexRequestBuilder.setSource(_, _) >> indexRequestBuilder
        1 * indexRequestBuilder.execute() >> future
        1 * future.actionGet(_)
    }

    def "Test save for #id throw other exception"() {
        def catalogName = 'prodhive'
        def databaseName = 'estestdb'
        def tableName = 'part'
        def id = 'prodhive/estestdb/part_test'
        def client = Mock(Client)
        def indexRequestBuilder = Mock(IndexRequestBuilder)
        def indexRequestBuilder2 = Mock(IndexRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def response = Mock(Response)
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "estestdb", "s3:/a/b")
        def ElasticSearchDoc doc = new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)

        when:
        es.save(Type.table.name(), id, doc)

        then:
        3 * client.prepareIndex(_, _, _) >> indexRequestBuilder
        3 * indexRequestBuilder.setSource(_,_) >> indexRequestBuilder
        3 * indexRequestBuilder.execute() >> { throw new TransportException("trans error")}
        1 * client.prepareIndex(_, "metacat-log") >> indexRequestBuilder2
        1 * indexRequestBuilder2.setSource(_ as Map) >> indexRequestBuilder2
        1 * indexRequestBuilder2.execute()  >> future
        1 * future.actionGet(_) >> response
    }

    def "Test save for #id throw other exception with log error"() {
        def catalogName = 'prodhive'
        def databaseName = 'estestdb'
        def tableName = 'part'
        def id = 'prodhive/estestdb/part_test'
        def client = Mock(Client)
        def indexRequestBuilder = Mock(IndexRequestBuilder)
        def indexRequestBuilder2 = Mock(IndexRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "estestdb", "s3:/a/b")
        def ElasticSearchDoc doc = new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)

        when:
        es.save(Type.table.name(), id, doc)

        then:
        3 * client.prepareIndex(_, _, _) >> indexRequestBuilder
        3 * indexRequestBuilder.setSource(_,_) >> indexRequestBuilder
        3 * indexRequestBuilder.execute() >> { throw new TransportException("trans error")}
        1 * client.prepareIndex(_, "metacat-log") >> indexRequestBuilder2
        1 * indexRequestBuilder2.setSource(_ as Map) >> indexRequestBuilder2
        1 * indexRequestBuilder2.execute()  >> future
        1 * future.actionGet(_) >> { throw new Exception("log error")}
    }

    def "Test delete for #id"() {
        def id = 'prodhive/estestdb/part_test'
        def client = Mock(Client)
        def deleteRequestBuilder = Mock(DeleteRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)

        when:
        es.delete(Type.table.name(), id)

        then:
        1 * client.prepareDelete(_, _, _) >> deleteRequestBuilder
        1 * deleteRequestBuilder.execute() >> future
        1 * future.actionGet(_)
    }

    def "Test delete for #id throw Exception"() {
        def id = 'prodhive/estestdb/part_test'
        def client = Mock(Client)
        def deleteRequestBuilder = Mock(DeleteRequestBuilder)
        def indexRequestBuilder2 = Mock(IndexRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def response = Mock(Response)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)

        when:
        es.delete(Type.table.name(), id)

        then:
        1 * client.prepareDelete(_, _, _) >> deleteRequestBuilder
        1 * deleteRequestBuilder.execute() >> { throw new IOException("ouch", new Throwable("Io exception")) }
        1 * client.prepareIndex(_, "metacat-log") >> indexRequestBuilder2
        1 * indexRequestBuilder2.setSource(_ as Map) >> indexRequestBuilder2
        1 * indexRequestBuilder2.execute()  >> future
        1 * future.actionGet(_) >> response
    }

    def "Test updates for #id"() {
        def catalogName = 'prodhive'
        def databaseName = 'estestdb'
        def tableName = 'part'
        def id = 'prodhive/estestdb/part_test'
        def client = Mock(Client)
        def bulkRequestBuilder = Mock(BulkRequestBuilder)
        def updateRequestBuilder = Mock(UpdateRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def bulkResponse = Mock(BulkResponse)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "estestdb", "s3:/a/b")
        def ElasticSearchDoc doc = new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false)

        when:
        es.updates(Type.table.name(),
            [id],
            metacatJson.toJsonObject(doc.getDto()))

        then:
        1 * client.prepareBulk() >> bulkRequestBuilder
        1 * client.prepareUpdate(_,_,_) >> updateRequestBuilder
        1 * updateRequestBuilder.setRetryOnConflict(3) >> updateRequestBuilder
        1 * updateRequestBuilder.setDoc(_,_) >> updateRequestBuilder
        1 * bulkRequestBuilder.add(_ as UpdateRequestBuilder)
        1 * bulkRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> bulkResponse
        1 * bulkResponse.hasFailures() >> false
    }

    def "Test updates for #id with failure"()  {
        def catalogName = 'prodhive'
        def databaseName = 'estestdb'
        def tableName = 'part'
        def id = 'prodhive/estestdb/part_test'
        def client = Mock(Client)
        def bulkRequestBuilder = Mock(BulkRequestBuilder)
        def updateRequestBuilder = Mock(UpdateRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def bulkResponse = Mock(BulkResponse)
        def failure = Mock(BulkItemResponse.Failure)
        failure.getCause() >> new Exception()
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "estestdb", "s3:/a/b")
        def ElasticSearchDoc doc = new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false)
        def bulkItemResponse = Mock(BulkItemResponse)


        when:
        es.updates(Type.table.name(),
            [id],
            metacatJson.toJsonObject(doc.getDto()))

        then:
        1 * client.prepareBulk() >> bulkRequestBuilder
        1 * client.prepareUpdate(_,_,_) >> updateRequestBuilder
        1 * updateRequestBuilder.setRetryOnConflict(3) >> updateRequestBuilder
        1 * updateRequestBuilder.setDoc(_,_) >> updateRequestBuilder
        1 * bulkRequestBuilder.add(_ as UpdateRequestBuilder)
        1 * bulkRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> bulkResponse
        1 * bulkResponse.hasFailures() >> true
        1 * bulkResponse.getItems() >> [bulkItemResponse]
        1 * bulkItemResponse.isFailed() >> true
        1 * bulkItemResponse.getFailure() >> failure
        1 * bulkItemResponse.getId() >> "1"
    }

    def "Test updates for #id with Exception "()  {
        def catalogName = 'prodhive'
        def databaseName = 'estestdb'
        def tableName = 'part'
        def id = 'prodhive/estestdb/part_test'
        def client = Mock(Client)
        def bulkRequestBuilder = Mock(BulkRequestBuilder)
        def updateRequestBuilder = Mock(UpdateRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "estestdb", "s3:/a/b")
        def ElasticSearchDoc doc = new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false)
        def indexRequestBuilder2 = Mock(IndexRequestBuilder)
        def response = Mock(Response)

        when:
        es.updates(Type.table.name(),
            [id],
            metacatJson.toJsonObject(doc.getDto()))

        then:
        1 * client.prepareBulk() >> bulkRequestBuilder
        1 * client.prepareUpdate(_,_,_) >> updateRequestBuilder
        1 * updateRequestBuilder.setRetryOnConflict(3) >> updateRequestBuilder
        1 * updateRequestBuilder.setDoc(_,_) >> updateRequestBuilder
        1 * bulkRequestBuilder.add(_ as UpdateRequestBuilder)
        1 * bulkRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> {throw new Exception("ouch", new Throwable("Exception")) }
        1 * client.prepareIndex(_, "metacat-log") >> indexRequestBuilder2
        1 * indexRequestBuilder2.setSource(_ as Map) >> indexRequestBuilder2
        1 * indexRequestBuilder2.execute()  >> future
        1 * future.actionGet(_) >> response
    }

    def "Test get for #id"() {
        def id = 'prodhive/estestdb/part_test'
        def client = Mock(Client)
        def getRequestBuilder = Mock(GetRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def response = Mock(GetResponse)
        def es = new ElasticSearchUtilImpl(client, config, Mock(MetacatJsonLocator), registry)
        def responseMap = Mock(Map)
        when:
        es.get(Type.table.name(), id)

        then:
        1 * client.prepareGet(_, _, _) >> getRequestBuilder
        1 * getRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> response
        2 * response.exists >> true
        1 * response.getSourceAsMap() >> responseMap
        1 * responseMap.get(ElasticSearchDoc.Field.USER) >> "user"
        1 * responseMap.get(ElasticSearchDoc.Field.DELETED) >> true
        1 * responseMap.get(ElasticSearchDoc.Field.TIMESTAMP) >> 12345l
        1 * response.getSourceAsBytes()
        1 * response.getType() >> "table"
        1 * response.getId() >> "user"
    }

    def "Test non-exist #id"() {
        def id = 'prodhive/estestdb/part_test'
        def client = Mock(Client)
        def getRequestBuilder = Mock(GetRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def response = Mock(GetResponse)
        def es = new ElasticSearchUtilImpl(client, config, Mock(MetacatJsonLocator), registry)
        when:
        es.get(Type.table.name(), id)

        then:
        1 * client.prepareGet(_, _, _) >> getRequestBuilder
        1 * getRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> response
        1 * response.exists >> false
    }

    def "Test getIdsByQualifiedName"() {
        def name = QualifiedName.fromString("testhive/test/testtable")
        def client = Mock(Client)
        def future = Mock(ListenableActionFuture)
        def response = Mock(SearchResponse)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)
        def searchRequestBuilder = Mock(SearchRequestBuilder)
        def searchHits = Mock(SearchHits)
        def hit = Mock(SearchHit)
        when:
        es.getIdsByQualifiedName(Type.table.name(), name)

        then:
        1 * client.prepareSearch(_) >> searchRequestBuilder
        1 * searchRequestBuilder.setTypes(_ as String) >> searchRequestBuilder
        1 * searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH) >> searchRequestBuilder
        1 * searchRequestBuilder.setQuery(_ as QueryBuilder) >> searchRequestBuilder
        1 * searchRequestBuilder.setSize(Integer.MAX_VALUE) >> searchRequestBuilder
        1 * searchRequestBuilder.setFetchSource(false) >> searchRequestBuilder
        1 * searchRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> response
        2 * response.getHits() >> searchHits
        2 * searchHits.getHits() >> [hit]
    }


    def "Test getQualifiedNamesByMarkerByNames"() {
        def names = [ QualifiedName.fromString("testhive/test/testtable") ]
        def instant = new Instant()
        def excludeQualifiedNames = []

        def client = Mock(Client)
        def future = Mock(ListenableActionFuture)
        def response = Mock(SearchResponse)
        def es = new ElasticSearchUtilImpl(client, config, Mock(MetacatJsonLocator), registry)
        def searchRequestBuilder = Mock(SearchRequestBuilder)
        def searchHits = Mock(SearchHits)
        def hit = Mock(SearchHit)
        when:
        es.getQualifiedNamesByMarkerByNames("table", names, instant, excludeQualifiedNames, String.class)

        then:
        1 * client.prepareSearch(_) >> searchRequestBuilder
        1 * searchRequestBuilder.setTypes(_ as String) >> searchRequestBuilder
        1 * searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH) >> searchRequestBuilder
        1 * searchRequestBuilder.setQuery(_ as QueryBuilder) >> searchRequestBuilder
        1 * searchRequestBuilder.setSize(Integer.MAX_VALUE) >> searchRequestBuilder
        1 * searchRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> response
        2 * response.getHits() >> searchHits
        2 * searchHits.getHits() >> [hit]
        1 *  hit.getSourceAsString() >> "test"
    }

    def "Test getTableIdsByUri" () {
        def uri = "s3://bucket/testhive/test/testtable"
        def client = Mock(Client)
        def future = Mock(ListenableActionFuture)
        def response = Mock(SearchResponse)
        def es = new ElasticSearchUtilImpl(client, config, Mock(MetacatJsonLocator), registry)
        def searchRequestBuilder = Mock(SearchRequestBuilder)
        def searchHits = Mock(SearchHits)
        def hit = Mock(SearchHit)
        when:
        es.getTableIdsByUri(Type.table.name(), uri)

        then:
        1 * client.prepareSearch(_) >> searchRequestBuilder
        1 * searchRequestBuilder.setTypes(_ as String) >> searchRequestBuilder
        1 * searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH) >> searchRequestBuilder
        1 * searchRequestBuilder.setQuery(_ as QueryBuilder) >> searchRequestBuilder
        1 * searchRequestBuilder.setSize(Integer.MAX_VALUE) >> searchRequestBuilder
        1 * searchRequestBuilder.setFetchSource(false) >> searchRequestBuilder
        1 * searchRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> response
        2 * response.getHits() >> searchHits
        2 * searchHits.getHits() >> [hit]
    }

    def "Test getTableIdsByCatalogs" () {
        def names = [ QualifiedName.fromString("testhive/test/testtable") ]
        def excludeQualifiedNames = []
        def client = Mock(Client)
        def future = Mock(ListenableActionFuture)
        def response = Mock(SearchResponse)
        def es = new ElasticSearchUtilImpl(client, config, Mock(MetacatJsonLocator), registry)
        def searchRequestBuilder = Mock(SearchRequestBuilder)
        def searchHits = Mock(SearchHits)
        def hit = Mock(SearchHit)
        when:
        es.getTableIdsByCatalogs(Type.table.name(), names, excludeQualifiedNames)

        then:
        1 * client.prepareSearch(_) >> searchRequestBuilder
        1 * searchRequestBuilder.setTypes(_ as String) >> searchRequestBuilder
        1 * searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH) >> searchRequestBuilder
        1 * searchRequestBuilder.setQuery(_ as QueryBuilder) >> searchRequestBuilder
        1 * searchRequestBuilder.setSize(Integer.MAX_VALUE) >> searchRequestBuilder
        1 * searchRequestBuilder.setFetchSource(false) >> searchRequestBuilder
        1 * searchRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> response
        2 * response.getHits() >> searchHits
        2 * searchHits.getHits() >> [hit]

    }

    def "Test simpleSearch" () {
        def names = [ QualifiedName.fromString("testhive/test/testtable") ]
        def excludeQualifiedNames = []
        def client = Mock(Client)
        def future = Mock(ListenableActionFuture)
        def response = Mock(SearchResponse)
        def es = new ElasticSearchUtilImpl(client, config, Mock(MetacatJsonLocator), registry)
        def searchRequestBuilder = Mock(SearchRequestBuilder)
        def searchHits = Mock(SearchHits)
        def hit = Mock(SearchHit)
        when:
        es.simpleSearch("simpleSearch")

        then:
        1 * client.prepareSearch(_) >> searchRequestBuilder
        1 * searchRequestBuilder.setTypes(_ as String) >> searchRequestBuilder
        1 * searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH) >> searchRequestBuilder
        1 * searchRequestBuilder.setQuery(_ as QueryBuilder) >> searchRequestBuilder
        1 * searchRequestBuilder.setSize(Integer.MAX_VALUE) >> searchRequestBuilder
        1 * searchRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> response
        2 * response.getHits() >> searchHits
        2 * searchHits.getHits() >> [hit]
        1 *  hit.getSourceAsString() >> "test"
    }

    def "Test softDelete" () {
        def ids = [ "testhive/test/testtable"]
        def client = Mock(Client)
        def bulkRequestBuilder = Mock(BulkRequestBuilder)
        def updateRequestBuilder = Mock(UpdateRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def bulkResponse = Mock(BulkResponse)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)
        def metacatConext = Mock(MetacatRequestContext)
        when:
        es.softDelete(Type.table.name(), ids, metacatConext)

        then:
        1 * metacatConext.getUserName() >> "test"
        1 * client.prepareBulk() >> bulkRequestBuilder
        1 * client.prepareUpdate(_,_,_) >> updateRequestBuilder
        1 * updateRequestBuilder.setRetryOnConflict(3) >> updateRequestBuilder
        1 * updateRequestBuilder.setDoc(_ as XContentBuilder) >> updateRequestBuilder
        1 * bulkRequestBuilder.add(_ as UpdateRequestBuilder)
        1 * bulkRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> bulkResponse
        1 * bulkResponse.hasFailures() >> false
    }

    def "Test softDelete with failure"()  {
        def ids = [ "testhive/test/testtable"]
        def client = Mock(Client)
        def bulkRequestBuilder = Mock(BulkRequestBuilder)
        def updateRequestBuilder = Mock(UpdateRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def failure = Mock(BulkItemResponse.Failure)
        failure.getCause() >> new Exception()
        def bulkResponse = Mock(BulkResponse)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)

        def bulkItemResponse = Mock(BulkItemResponse)

        def metacatConext = Mock(MetacatRequestContext)
        when:
        es.softDelete(Type.table.name(), ids, metacatConext)

        then:
        1 * metacatConext.getUserName() >> "test"
        1 * client.prepareBulk() >> bulkRequestBuilder
        1 * client.prepareUpdate(_,_,_) >> updateRequestBuilder
        1 * updateRequestBuilder.setRetryOnConflict(3) >> updateRequestBuilder
        1 * updateRequestBuilder.setDoc(_ as XContentBuilder) >> updateRequestBuilder
        1 * bulkRequestBuilder.add(_ as UpdateRequestBuilder)
        1 * bulkRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> bulkResponse
        1 * bulkResponse.hasFailures() >> true
        1 * bulkResponse.getItems() >> [bulkItemResponse]
        1 * bulkItemResponse.isFailed() >> true
        1 * bulkItemResponse.getFailure() >> failure
        1 * bulkItemResponse.getId() >> "1"
    }

    def "Test softDelete with Exception "()  {
        def ids = [ "testhive/test/testtable"]
        def client = Mock(Client)
        def bulkRequestBuilder = Mock(BulkRequestBuilder)
        def updateRequestBuilder = Mock(UpdateRequestBuilder)
        def future = Mock(ListenableActionFuture)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)
        def indexRequestBuilder2 = Mock(IndexRequestBuilder)
        def metacatConext = Mock(MetacatRequestContext)
        def response = Mock(Response)

        when:
        es.softDelete(Type.table.name(), ids, metacatConext)

        then:
        1 * metacatConext.getUserName() >> "test"
        1 * client.prepareBulk() >> bulkRequestBuilder
        1 * client.prepareUpdate(_,_,_) >> updateRequestBuilder
        1 * updateRequestBuilder.setRetryOnConflict(3) >> updateRequestBuilder
        1 * updateRequestBuilder.setDoc(_ as XContentBuilder) >> updateRequestBuilder
        1 * bulkRequestBuilder.add(_ as UpdateRequestBuilder)
        1 * bulkRequestBuilder.execute() >> future
        1 * future.actionGet(_) >> {throw new Exception("ouch", new Throwable("Exception")) }
        1 * client.prepareIndex(_, "metacat-log") >> indexRequestBuilder2
        1 * indexRequestBuilder2.setSource(_ as Map) >> indexRequestBuilder2
        1 * indexRequestBuilder2.execute()  >> future
        1 * future.actionGet(_) >> response
    }

    def "Test toJsonString" () {
        def catalogName = 'prodhive'
        def databaseName = 'estestdb'
        def tableName = 'part'
        def client = Mock(Client)
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "estestdb", "s3:/a/b")
        def ElasticSearchDoc doc = new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false)
        def es = new ElasticSearchUtilImpl(client, config, metacatJson, registry)
        when:
        String str = es.toJsonString(doc)
        ObjectNode nodes = metacatJson.parseJsonObject(str)

        then:
        nodes.get("audit").size() == 4
        nodes.get("fields").size() == 4
        nodes.get("timestamp") != null
        nodes.get("user_") != null
        nodes.get("deleted_") != null
        nodes.get(ElasticSearchDocConstants.DEFINITION_METADATA).size() == 18
        nodes.get(ElasticSearchDoc.Field.SEARCHABLE_DEFINITION_METADATA).size() == 16
    }
}
