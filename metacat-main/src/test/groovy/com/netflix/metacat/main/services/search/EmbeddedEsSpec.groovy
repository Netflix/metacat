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
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.testdata.provider.DataDtoProvider
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.InternalSettingsPreparer
import org.joda.time.Instant
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
import com.netflix.metacat.main.services.search.ElasticSearchDoc.Type

/**
 * Embedded elasticSearch Cluster for testing.
 * @author zhenl
 * @since 1.1.0
 */
//TODO: remove after setting up integration ElasticSearch test in real cluster or other form
class EmbeddedEsSpec extends Specification {
    @Shared
    Config config = Mock(Config)
    @Shared
    Config config2 = Mock(Config)
    @Shared
    ElasticSearchUtilImpl es
    @Shared
    ElasticSearchUtilImpl esMig
    @Shared
    MetacatJson metacatJson
    @Shared
    MetacatRequestContext metacatContext = MetacatRequestContext.builder().
        userName("test").
        clientAppName("testApp").
        clientId("testClientId").
        jobId("testJobId").build()

    @Shared
    String esIndex = "metacat"
    @Shared
    String esMergeIndex = "metacat_v2"
    @Shared
    ElasticSearchMetric elasticSearchMetric

    @Shared
    org.elasticsearch.node.Node node
    def setupSpec() {
        Settings settings = Settings.builder()
            .put("http.enabled", false)
            .put("transport.type", "local")
            .put("path.home", "./")
            .put("path.data", "./")
            .build();
        node = new org.elasticsearch.node.Node(InternalSettingsPreparer.prepareSettings(settings));
        node.start()
        Client client = node.client()

        String[] indices = [esIndex, esMergeIndex];
        for (String _index : indices) {
            if (client.admin().indices().exists(new IndicesExistsRequest(_index)).actionGet().exists) {
                client.admin().indices().delete(new DeleteIndexRequest(_index)).actionGet()
            }
        }
        // Create a new index
        for (String _index : indices) {
            def index = new CreateIndexRequest(_index)
            index.source(getFile('metacat.json').getText())
            client.admin().indices().create(index).actionGet()
        }

        metacatJson = new MetacatJsonLocator()
        config.getEsIndex() >> esIndex
        config.getElasticSearchCallTimeout() >> 10
        config.getElasticSearchBulkCallTimeout() >> 10
        config.isElasticSearchPublishMetacatLogEnabled() >> true
        es = new ElasticSearchUtilImpl(client, config, metacatJson, elasticSearchMetric)

        config2.getEsIndex() >> esIndex
        config2.getMergeEsIndex() >> esMergeIndex
        config2.getElasticSearchCallTimeout() >> 10
        config2.getElasticSearchBulkCallTimeout() >> 10
        config2.isElasticSearchPublishMetacatLogEnabled() >> true
        esMig = new ElasticSearchUtilImpl(client, config2, metacatJson, elasticSearchMetric)
    }

    def cleanupSpec() {
        node.close()
        println "cleanup es node"
    }

    def getFile(String name) {
        def f = new File('../metacat-main/src/test/resources/search/mapping/' + name)
        if (!f.exists()) {
            f = new File('metacat-main/src/test/resources/search/mapping/' + name)
        }
        return f
    }

    @Unroll
    def "Test save for #id"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b")
        es.save(Type.table.name(), id, new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false))
        def result = (TableDto) es.get(Type.table.name(), id).getDto()
        expect:
        id == result.getName().toString()
        where:
        catalogName | databaseName | tableName | id
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
    }

    @Unroll
    def "Test save and getQualifiedNamesByMarkerByNames for #id"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b/c")
        es.save(Type.table.name(), [new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false)])
        def result = (TableDto) es.get(Type.table.name(), id).getDto()
        def result2 = es.getQualifiedNamesByMarkerByNames(Type.table.name(),
                                           [QualifiedName.fromString(id)],
                                            Instant.now().toInstant(),
                                            new ArrayList<QualifiedName>(),TableDto.class )

        expect:
        id == result.getName().toString()
        result2.size() == 1
        where:
        catalogName | databaseName | tableName   | id
        'prodhive'  | 'amajumdar'  | 'part_test' | 'prodhive/amajumdar/part_test'
    }

    @Unroll
    def "Test delete for #id"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b")
        es.save(Type.table.name(), id,  new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false))
        softDelete ? es.softDelete(Type.table.name(), id, metacatContext) : es.delete(Type.table.name(), id)
        def result = es.get(Type.table.name(), id, esIndex)
        expect:
        if (softDelete) {
            result.isDeleted()
            id == ((TableDto) result.getDto()).getName().toString()
        } else {
            result == null
        }
        where:
        catalogName | databaseName | tableName | id                        | softDelete
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part' | false
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part' | true
    }

    @Unroll
    def "Test deletes for #id"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b")
        es.save(Type.table.name(), id,  new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false))
        softDelete ? es.softDelete(Type.table.name(), [id], metacatContext) : es.delete(Type.table.name(), [id])
        def result = es.get(Type.table.name(), id)
        expect:
        if (softDelete) {
            result.isDeleted()
            id == ((TableDto) result.getDto()).getName().toString()
        } else {
            result == null
        }
        where:
        catalogName | databaseName | tableName | id                        | softDelete
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part' | false
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part' | true
    }

    @Unroll
    def "Test deletes for #type"() {
        given:
        def tables = DataDtoProvider.getTables(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b", noOfTables)
        def docs = tables.collect {
            String userName = it.getAudit() != null ? it.getAudit().getCreatedBy()
                : "admin";
            return new ElasticSearchDoc(it.getName().toString(), it, userName, false, null)
        }
        es.save(Type.table.name(), docs)
        es.refresh()
        es.delete(MetacatRequestContext.builder().userName("testUpdate").build(), Type.table.name(), softDelete)
        where:
        catalogName | databaseName | tableName | noOfTables | softDelete
        'prodhive'  | 'amajumdar'  | 'part'    | 10         | false
        'prodhive'  | 'amajumdar'  | 'part'    | 0          | false
        'prodhive'  | 'amajumdar'  | 'part'    | 1000       | false
        'prodhive'  | 'amajumdar'  | 'part'    | 10         | true
        'prodhive'  | 'amajumdar'  | 'part'    | 0          | true
    }

    @Unroll
    def "Test migSave for #id"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b")
        esMig.save(Type.table.name(), id, new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false))
        for (String index : [esIndex, esMergeIndex]) {
            def result = (TableDto) es.get(Type.table.name(), id, index).getDto()
            expect:
            id == result.getName().toString()
        }
        where:
        catalogName | databaseName | tableName | id
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
    }

    @Unroll
    def "Test migSave for list of #id"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b")
        esMig.save(Type.table.name(), [new ElasticSearchDoc(table.name.toString(), table, metacatContext.userName, false)])
        for (String index : [esIndex, esMergeIndex]) {
            def result = (TableDto) es.get(Type.table.name(), id, index).getDto()
            expect:
            id == result.getName().toString()
        }
        where:
        catalogName | databaseName | tableName | id
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
    }

    @Unroll
    def "Test migSoftDelete for #id that does not exists in mergeIndex"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b")
        es.save(Type.table.name(), id, new ElasticSearchDoc(table.getName().toString(), table,
            "testuser", false))
        esMig.softDelete(Type.table.name(), id, metacatContext)
        for (String index : [esIndex, esMergeIndex]) {
            def result = es.get(Type.table.name(), id, index)
            expect:
            result.isDeleted()
            id == ((TableDto) result.getDto()).getName().toString()
        }
        where:
        catalogName | databaseName | tableName | id
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
    }

    @Unroll
    def "Test migSoftDeletes for list of #id that exists in mergeIndex"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b")
        def docs = [new ElasticSearchDoc(table.name.toString(), table, metacatContext.userName, false),
                    new ElasticSearchDoc(table.name.toString(), table, metacatContext.userName, false),
                    new ElasticSearchDoc(table.name.toString(), table, metacatContext.userName, false)]
        esMig.save(Type.table.name(), docs)
        esMig.softDelete(Type.table.name(), [id], metacatContext)
        for (String index : [esIndex, esMergeIndex]) {
            def result = es.get(Type.table.name(), id, index)
            expect:
            result.isDeleted()
            id == ((TableDto) result.getDto()).getName().toString()
        }
        where:
        catalogName | databaseName | tableName | id
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
    }

    @Unroll
    def "Test migSoftDeletes for list of #id that do not exist in mergeIndex"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, "amajumdar", "s3:/a/b")
        def docs = [new ElasticSearchDoc(table.name.toString(), table, metacatContext.userName, false),
                    new ElasticSearchDoc(table.name.toString(), table, metacatContext.userName, false),
                    new ElasticSearchDoc(table.name.toString(), table, metacatContext.userName, false)]
        es.save(Type.table.name(), docs)
        esMig.softDelete(Type.table.name(), [id], metacatContext)
        for (String index : [esIndex, esMergeIndex]) {
            def result = es.get(Type.table.name(), id, index)
            expect:
            result.isDeleted()
            id == ((TableDto) result.getDto()).getName().toString()
        }
        where:
        catalogName | databaseName | tableName | id
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part'
    }

    @Unroll
    def "Test updates for #id "() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, metacatContext.getUserName(), uri)
        es.save(Type.table.name(), id, new ElasticSearchDoc(table.getName().toString(), table,
            "testUpdate", false))
        es.updates(Type.table.name(), [id], new MetacatJsonLocator().parseJsonObject('{"dataMetadata": {"metrics":{"count":10}}}'))
        def result = es.get(Type.table.name(), id, esIndex)
        def resultByUri = es.getTableIdsByUri(Type.table.name(), uri)
        expect:
        result != null
        result.getUser() == "testUpdate"
        ((TableDto) result.getDto()).getDataMetadata() != null
        resultByUri != null
        resultByUri.size() == 1
        resultByUri[0] == id

        where:
        catalogName | databaseName | tableName | id                        | uri
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part' | 's3:/a/b'
    }

    def "Test ElasticSearchDoc addSearchableDefinitionMetadata"() {
        given:
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, metacatContext.getUserName(), uri)
        ElasticSearchDoc doc = new ElasticSearchDoc("test", table, "zhenl", false);
        ObjectNode oMetadata = new MetacatJsonLocator().toJsonObject(table);
        doc.addSearchableDefinitionMetadata(oMetadata);
        expect:
        ObjectNode node = oMetadata.get("searchableDefinitionMetadata");
        node.get("owner") != null
        node.get("lifetime") != null
        node.get("extendedSchema") != null
        where:
        catalogName | databaseName | tableName | id                        | uri
        'prodhive'  | 'amajumdar'  | 'part'    | 'prodhive/amajumdar/part' | 's3:/a/b'
    }

}
