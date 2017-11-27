/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */
package com.netflix.metacat

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.guava.GuavaModule
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule
import com.netflix.metacat.client.api.MetacatV1
import com.netflix.metacat.client.api.MetadataV1
import com.netflix.metacat.client.api.PartitionV1
import com.netflix.metacat.client.api.TagV1
import com.netflix.metacat.client.module.JacksonDecoder
import com.netflix.metacat.client.module.JacksonEncoder
import com.netflix.metacat.client.module.MetacatErrorDecoder
import com.netflix.metacat.common.MetacatRequestContext
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException
import com.netflix.metacat.common.exception.MetacatBadRequestException
import com.netflix.metacat.common.exception.MetacatNotFoundException
import com.netflix.metacat.common.exception.MetacatNotSupportedException
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.testdata.provider.PigDataDtoProvider
import feign.*
import feign.jaxrs.JAXRSContract
import feign.slf4j.Slf4jLogger
import groovy.sql.Sql
import spock.lang.Ignore
import org.joda.time.Instant
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

/**
 * MetacatSmokeSpec.
 * @author amajumdar
 * @author zhenl
 * @since 1.0.0
 */
class MetacatSmokeSpec extends Specification {
    public static MetacatV1 api
    public static PartitionV1 partitionApi
    public static MetadataV1 metadataApi
    public static TagV1 tagApi
    public static MetacatJson metacatJson = new MetacatJsonLocator()

    def setupSpec() {
        String url = "http://localhost:${System.properties['metacat_http_port']}"
        assert url, 'Required system property "metacat_url" is not set'

        ObjectMapper mapper = metacatJson.getPrettyObjectMapper().copy()
            .registerModule(new GuavaModule())
            .registerModule(new JaxbAnnotationModule())
        RequestInterceptor interceptor = new RequestInterceptor() {
            @Override
            void apply(RequestTemplate template) {
                template.header(MetacatRequestContext.HEADER_KEY_USER_NAME, "metacat-test")
                template.header(MetacatRequestContext.HEADER_KEY_CLIENT_APP_NAME, "metacat-test")
            }
        }
        api = Feign.builder()
            .logger(new Slf4jLogger())
            .contract(new JAXRSContract())
            .encoder(new JacksonEncoder(mapper))
            .decoder(new JacksonDecoder(mapper))
            .errorDecoder(new MetacatErrorDecoder(metacatJson))
            .requestInterceptor(interceptor)
            .retryer(new Retryer.Default(TimeUnit.MINUTES.toMillis(30), TimeUnit.MINUTES.toMillis(30), 0))
            .options(new Request.Options((int) TimeUnit.MINUTES.toMillis(10), (int) TimeUnit.MINUTES.toMillis(30)))
            .target(MetacatV1.class, url)
        partitionApi = Feign.builder()
            .logger(new Slf4jLogger())
            .contract(new JAXRSContract())
            .encoder(new JacksonEncoder(mapper))
            .decoder(new JacksonDecoder(mapper))
            .errorDecoder(new MetacatErrorDecoder(metacatJson))
            .requestInterceptor(interceptor)
            .retryer(new Retryer.Default(TimeUnit.MINUTES.toMillis(30), TimeUnit.MINUTES.toMillis(30), 0))
            .options(new Request.Options((int) TimeUnit.MINUTES.toMillis(10), (int) TimeUnit.MINUTES.toMillis(30)))
            .target(PartitionV1.class, url)
        tagApi = Feign.builder()
            .logger(new Slf4jLogger())
            .contract(new JAXRSContract())
            .encoder(new JacksonEncoder(mapper))
            .decoder(new JacksonDecoder(mapper))
            .errorDecoder(new MetacatErrorDecoder(metacatJson))
            .requestInterceptor(interceptor)
            .retryer(new Retryer.Default(TimeUnit.MINUTES.toMillis(30), TimeUnit.MINUTES.toMillis(30), 0))
            .options(new Request.Options((int) TimeUnit.MINUTES.toMillis(10), (int) TimeUnit.MINUTES.toMillis(30)))
            .target(TagV1.class, url)
        metadataApi = Feign.builder()
            .logger(new Slf4jLogger())
            .contract(new JAXRSContract())
            .encoder(new JacksonEncoder(mapper))
            .decoder(new JacksonDecoder(mapper))
            .errorDecoder(new MetacatErrorDecoder(metacatJson))
            .requestInterceptor(interceptor)
            .retryer(new Retryer.Default(TimeUnit.MINUTES.toMillis(30), TimeUnit.MINUTES.toMillis(30), 0))
            .options(new Request.Options((int) TimeUnit.MINUTES.toMillis(10), (int) TimeUnit.MINUTES.toMillis(30)))
            .target(MetadataV1.class, url)
    }

    @Shared
    boolean isLocalEnv = Boolean.valueOf(System.getProperty("local", "true"))

    static createTable(String catalogName, String databaseName, String tableName) {
        def catalog = api.getCatalog(catalogName)
        if (!catalog.databases.contains(databaseName)) {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        }
        def database = api.getDatabase(catalogName, databaseName, false, true)
        def owner = 'amajumdar'
        def uri = null
        if (Boolean.valueOf(System.getProperty("local", "true"))) {
            uri = String.format('file:/tmp/%s/%s', databaseName, tableName)
        }
        if (!database.getTables().contains(tableName)) {
            def newTable
            if ('part' == tableName) {
                newTable = PigDataDtoProvider.getPartTable(catalogName, databaseName, owner, uri)
            } else if ('parts' == tableName) {
                newTable = PigDataDtoProvider.getPartsTable(catalogName, databaseName, owner, uri)
            } else if ('metacat_all_types' == tableName) {
                newTable = PigDataDtoProvider.getMetacatAllTypesTable(catalogName, databaseName, owner, uri)
            } else {
                newTable = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, owner, uri)
            }
            api.createTable(catalogName, databaseName, tableName, newTable)
        }
    }

    def createTable() {
        when:
        try {
            api.createDatabase('embedded-hive-metastore', 'smoke_db', new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        try {
            api.createDatabase('embedded-hive-metastore', 'franklinviews', new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        try {
            api.createDatabase('embedded-fast-hive-metastore', 'fsmoke_db', new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        try {
            api.createDatabase('embedded-fast-hive-metastore', 'franklinviews', new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        try {
            api.createDatabase('hive-metastore', 'smoke_db', new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        try {
            api.createDatabase('hive-metastore', 'franklinviews', new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        createTable('embedded-hive-metastore', 'smoke_db', 'part')
        createTable('embedded-hive-metastore', 'smoke_db', 'parts')
        createTable('embedded-fast-hive-metastore', 'fsmoke_db', 'part')
        createTable('embedded-fast-hive-metastore', 'fsmoke_db', 'parts')
        createTable('hive-metastore', 'hsmoke_db', 'part')
        createTable('hive-metastore', 'hsmoke_db', 'parts')

        try {
            api.createDatabase('s3', 'smoke_db', new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        createTable('s3-mysql-db', 'smoke_db', 'part')
        then:
        noExceptionThrown()
    }

    def createAllTypesTable() {
        when:
        createTable('embedded-hive-metastore', 'smoke_db', 'metacat_all_types')
        def table = api.getTable('embedded-hive-metastore', 'smoke_db', 'metacat_all_types', true, true, true)
        then:
        noExceptionThrown()
        table.fields.find { it.name == 'latest_is_available' }.type == '{(array_element: map[chararray])}'
    }

    def testGetCatalogNames() {
        when:
        def catalogs = api.getCatalogNames()
        def catalogNames = catalogs.collect { it.catalogName }
        then:
        catalogNames.size() > 0
        catalogNames.contains('embedded-hive-metastore')
        catalogNames.contains('embedded-fast-hive-metastore')
        catalogNames.contains('s3-mysql-db')
        catalogNames.contains('hive-metastore')
    }

    def testCreateCatalog() {
        when:
        api.createCatalog(new CreateCatalogDto())
        then:
        thrown(MetacatNotSupportedException)
    }

    @Unroll
    def "Test create database for #catalogName/#databaseName"() {
        given:
        def definitionMetadata = (ObjectNode) metacatJson.emptyObjectNode().set("owner", metacatJson.emptyObjectNode())
        expect:
        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto(definitionMetadata: definitionMetadata))
            error == null
        } catch (Exception e) {
            e.class == error
        }
        if (!error) {
            def database = api.getDatabase(catalogName, databaseName, true, true)
            assert database != null && database.name.databaseName == databaseName
            assert database.definitionMetadata != null && database.definitionMetadata == definitionMetadata
        }
        cleanup:
        if (!error) {
            api.deleteDatabase(catalogName, databaseName)
        }
        where:
        catalogName                     | databaseName | error
        'embedded-hive-metastore'       | 'smoke_db0'  | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db0' | null
        'hive-metastore'                | 'hsmoke_db0' | null
        's3-mysql-db'                   | 'smoke_db0'  | null
        'invalid-catalog'               | 'smoke_db0'  | MetacatNotFoundException.class
    }

    @Unroll
    def "Test create table for #catalogName/#databaseName/#tableName(setUri:#setUri)"() {
        given:
        def updatedUri = String.format('file:/tmp/%s/%s/%s', databaseName, tableName, 'updated')
        expect:
        try {
            try {
                api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
            } catch (Exception ignored) {
            }
            def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
            def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'amajumdar', uri)
            if (!setUri) {
                tableDto.getSerde().setUri(null)
            }
            if(setNull) {
                tableDto.getSerde().setInputFormat(null)
                tableDto.getSerde().setOutputFormat(null)
                tableDto.getSerde().setSerializationLib(null)
            }
            api.createTable(catalogName, databaseName, tableName, tableDto)
            tableDto.getSerde().setUri(updatedUri)
            api.updateTable(catalogName, databaseName, tableName, tableDto)
            error == null
        } catch (Exception e) {
            e.class == error
        }
        if (!error) {
            def table = api.getTable(catalogName, databaseName, tableName, true, true, false)
            assert table != null && table.name.tableName == tableName
            assert table.getDefinitionMetadata() != null
            assert table.getSerde().getUri() == updatedUri
            //TODO: assert table.getSerde().getParameters().size() == 0
            assert table.getSerde().getSerdeInfoParameters().size() >= 1
            assert table.getSerde().getSerdeInfoParameters().get('serialization.format') == '1'
        }
        cleanup:
        if (!error) {
            api.deleteTable(catalogName, databaseName, tableName)
        }
        where:
        catalogName                     | databaseName | tableName           | setUri | setNull | error
        'embedded-hive-metastore'       | 'smoke_db1'  | 'test_create_table' | true   | false   | null
        'embedded-hive-metastore'       | 'smoke_db1'  | 'test_create_table' | false  | false   | null
        'embedded-hive-metastore'       | 'smoke_db1'  | 'test_create_table1'| false  | true    | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db1' | 'test_create_table' | true   | false   | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db1' | 'test_create_table' | false  | false   | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db1' | 'test_create_table1'| false  | true    | null
        'hive-metastore'                | 'hsmoke_db1' | 'test_create_table' | true   | false   | null
        'hive-metastore'                | 'hsmoke_db1' | 'test_create_table' | false  | false   | null
        'hive-metastore'                | 'hsmoke_db1' | 'test_create_table1'| false  | true    | null
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table' | true   | false   | null
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table.definition' | true   | false   | null   //verify the table name can have . character
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table.knp' | true   | false   | null   //verify the table name can have knp extension
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table.mp3' | true   | false   | null   //verify the table name can have knp extension
        'invalid-catalog'               | 'smoke_db1'  | 'z'                 | true   | false   | MetacatNotFoundException.class
    }

    @Unroll
    def "Test copy table as #catalogName/#databaseName/metacat_all_types_copy"() {
        given:
        createTable(catalogName, databaseName, 'metacat_all_types')
        def table = api.getTable(catalogName, databaseName, 'metacat_all_types', true, true, false)
        table.setName(QualifiedName.ofTable(catalogName, databaseName, 'metacat_all_types_copy'))
        api.createTable(catalogName, databaseName, 'metacat_all_types_copy', table)
        def copyTable = api.getTable(catalogName, databaseName, 'metacat_all_types_copy', true, true, false)
        expect:
        table.fields == copyTable.fields
        cleanup:
        api.deleteTable(catalogName, databaseName, 'metacat_all_types')
        api.deleteTable(catalogName, databaseName, 'metacat_all_types_copy')
        where:
        catalogName                     | databaseName
        'embedded-hive-metastore'       | 'smoke_db1'
        'embedded-fast-hive-metastore'  | 'fsmoke_db1'
        'hive-metastore'                | 'hsmoke_db1'
        's3-mysql-db'                   | 'smoke_db1'
    }

    /**
     * This test case validates the mixed case of definition names in user metadata. Added after we came across
     * a regression issue with Aegisthus job.
     */
    @Unroll
    def "Test get table for #catalogName/#databaseName/#tableName"() {
        given:
        createTable(catalogName, databaseName, tableName)
        def table = api.getTable(catalogName, databaseName, tableName, true, true, false)
        expect:
        table.getSerde() != null
        table.getDefinitionMetadata() != null
        table.getDefinitionMetadata().size() > 0
        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)
        where:
        catalogName                     | databaseName | tableName
        'embedded-hive-metastore'       | 'smoke_db2'  | 'part'
        'embedded-fast-hive-metastore'  | 'fsmoke_db2' | 'part'
        'hive-metastore'                | 'hsmoke_db2' | 'part'
        's3-mysql-db'                   | 'smoke_db2'  | 'part'
        's3-mysql-db'                   | 'smoke_db2'  | 'PART'
    }

    @Unroll
    def "Test rename table for #catalogName/#databaseName/#tableName"() {
        expect:
        try {
            createTable(catalogName, databaseName, tableName)
            api.renameTable(catalogName, databaseName, tableName, newTableName)
            error == null
        } catch (Exception e) {
            e.class == error
        }
        if (!error) {
            def table = api.getTable(catalogName, databaseName, newTableName, false, true, false)
            assert table != null && table.name.tableName == newTableName
            assert table.getDefinitionMetadata() != null
        }
        cleanup:
        if (!error) {
            api.deleteTable(catalogName, databaseName, newTableName)
        }
        where:
        catalogName                     | databaseName | tableName           | error | newTableName
        'embedded-hive-metastore'       | 'smoke_db3'  | 'test_create_table' | null  | 'test_create_table1'
        'embedded-fast-hive-metastore'  | 'fsmoke_db3' | 'test_create_table' | null  | 'test_create_table1'
        'hive-metastore'                | 'hsmoke_db3' | 'test_create_table' | null  | 'test_create_table1'
    }

    @Unroll
    def "Test create view for #catalogName/#databaseName/#tableName/#viewName"() {
        expect:
        try {
            try {
                api.createDatabase(catalogName, 'franklinviews', new DatabaseCreateRequestDto())
            } catch (Exception ignored) {
            }
            createTable(catalogName, databaseName, tableName)
            api.createMView(catalogName, databaseName, tableName, viewName, true, null)
            if (repeat) {
                api.createMView(catalogName, databaseName, tableName, viewName, true, null)
            }
            error == null
        } catch (Exception e) {
            e.class == error
        }
        if (!error) {
            def view = api.getMView(catalogName, databaseName, tableName, viewName)
            assert view != null && view.name.tableName == tableName
        }
        cleanup:
        if (!error) {
            api.deleteMView(catalogName, databaseName, tableName, viewName)
        }
        where:
        catalogName                     | databaseName | tableName           | viewName    | error                          | repeat
        'embedded-hive-metastore'       | 'smoke_db4'  | 'part'              | 'part_view' | null                           | false
        'embedded-hive-metastore'       | 'smoke_db4'  | 'part'              | 'part_view' | null                           | true
        'embedded-fast-hive-metastore'  | 'fsmoke_db4' | 'part'              | 'part_view' | null                           | false
        'embedded-fast-hive-metastore'  | 'fsmoke_db4' | 'part'              | 'part_view' | null                           | true
        'hive-metastore'                | 'hsmoke_db4' | 'part'              | 'part_view' | null                           | false
        'hive-metastore'                | 'hsmoke_db4' | 'part'              | 'part_view' | null                           | true
        'embedded-hive-metastore'       | 'smoke_db4'  | 'metacat_all_types' | 'part_view' | null                           | false
        'embedded-fast-hive-metastore'  | 'fsmoke_db4' | 'metacat_all_types' | 'part_view' | null                           | false
        's3-mysql-db'                   | 'smoke_db4'  | 'part'              | 'part_view' | null                           | false
        'xyz'                           | 'smoke_db4'  | 'z'                 | 'part_view' | MetacatNotFoundException.class | false
    }

    @Unroll
    def "Test create view without snapshot for #catalogName/#databaseName/#tableName/#viewName"() {
        expect:
        try {
            try {
                api.createDatabase(catalogName, 'franklinviews', new DatabaseCreateRequestDto())
            } catch (Exception ignored){}
            createTable(catalogName, databaseName, tableName)
            api.createMView(catalogName, databaseName, tableName, viewName, null, null);
            if (repeat) {
                api.createMView(catalogName, databaseName, tableName, viewName, null, null);
            }
            error == null
        } catch (Exception e) {
            e.class == error
        }
        if (!error) {
            def view = api.getMView(catalogName, databaseName, tableName, viewName)
            assert view != null && view.name.tableName == tableName
        }
        cleanup:
        if (!error) {
            api.deleteMView(catalogName, databaseName, tableName, viewName)
        }
        where:
        catalogName                     | databaseName   | tableName           | viewName    | error                          | repeat
        'embedded-hive-metastore'       | 'smoke_db4'    | 'part'              | 'part_view' | null                           | false
        'embedded-hive-metastore'       | 'smoke_db4'    | 'part'              | 'part_view' | null                           | true
        'embedded-fast-hive-metastore'  | 'fsmoke_db4'   | 'part'              | 'part_view' | null                           | false
        'embedded-fast-hive-metastore'  | 'fsmoke_db4'   | 'part'              | 'part_view' | null                           | true
        'hive-metastore'                | 'hsmoke_db4'   | 'part'              | 'part_view' | null                           | false
        'hive-metastore'                | 'hsmoke_db4'   | 'part'              | 'part_view' | null                           | true
        'embedded-hive-metastore'       | 'smoke_db4'    | 'metacat_all_types' | 'part_view' | null                           | false
        's3-mysql-db'                   | 'smoke_db4'    | 'part'              | 'part_view' | null                           | false
        'xyz'                           | 'smoke_db4'    | 'z'                 | 'part_view' | MetacatNotFoundException.class | false
    }

    @Unroll
    def "Test save 0 partitions for #catalogName/#databaseName/#tableName"() {
        when:
        createTable(catalogName, databaseName, tableName)
        partitionApi.savePartitions(catalogName, databaseName, tableName, new PartitionsSaveRequestDto())
        then:
        noExceptionThrown()
        where:
        catalogName                     | databaseName      | tableName
        'embedded-hive-metastore'       | 'smoke_db'        | 'part'
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'
        'hive-metastore'                | 'hsmoke_db'       | 'part'
        's3-mysql-db'                   | 'smoke_db'        | 'part'
    }

    @Unroll
    def "Test('#repeat') save partitions for #catalogName/#databaseName/#tableName with partition name starting with #partitionName"() {
        expect:
        def uri = isLocalEnv ? 'file:/tmp/abc' : String.format("s3://wh/%s.db/%s", databaseName, tableName)
        def createDate = Instant.now().toDate()
        try {
            createTable(catalogName, databaseName, tableName)
            def partition = PigDataDtoProvider.getPartition(catalogName, databaseName, tableName, partitionName, uri + uriSuffix)
            def request = new PartitionsSaveRequestDto(partitions: [partition])

            partitionApi.savePartitions(catalogName, databaseName, tableName, request)
            if (repeat) {
                partition.getSerde().setUri(uri + '0' + uriSuffix)
                if (alter) {
                    request.setAlterIfExists(true)
                }
                request.setDefinitionMetadata((ObjectNode) metacatJson.emptyObjectNode().set('savePartitions', metacatJson.emptyObjectNode()))
                partitionApi.savePartitions(catalogName, databaseName, tableName, request)
                def savedPartitions = partitionApi.getPartitions(catalogName, databaseName, tableName, partitionName.replace('=', '="') + '"', null, null, null, null, false)
                createDate = savedPartitions.get(0).getAudit().createdDate
                request.getPartitions().get(0).setDataMetadata(metacatJson.emptyObjectNode().put('updateMetadata', -1))
                request.setSaveMetadataOnly(true)
                partitionApi.savePartitions(catalogName, databaseName, tableName, request)
            }
            error == null
        } catch (Exception e) {
            e.class == error
        }
        if (!error) {
            //To test the case that double quoats are supported
            def partitions = partitionApi.getPartitions(catalogName, databaseName, tableName, partitionName.replace('=', '="') + '"', null, null, null, null, true)
            assert partitions != null && partitions.size() == 1 && partitions.find {
                it.name.partitionName == partitionName
            } != null
            def partitionDetails = partitionApi.getPartitionsForRequest(catalogName, databaseName, tableName, null, null, null, null, true, new GetPartitionsRequestDto(filter: partitionName.replace('=', '="') + '"', includePartitionDetails: true))
            def partitionDetail = partitionDetails.find { it.name.partitionName == partitionName }
            assert partitionDetails != null && partitionDetails.size() == 1 && partitionDetail != null && partitionDetails.size() == partitions.size() && partitionDetail.getSerde().getSerdeInfoParameters().size() >= 1
            if (catalogName != 's3-mysql-db') {
                assert partitionDetail.getMetadata().size() >= 1
                assert partitionDetail.getMetadata().get('functionalTest') == 'true'
            }
            if (repeat) {
                assert api.getTable(catalogName, databaseName, tableName, false, true, false).getDefinitionMetadata().get('savePartitions') != null
                assert partitions.get(0).dataUri == uri + '0' + uriResult
                assert partitions.get(0).getDataMetadata().get('updateMetadata') != null
                assert partitions.get(0).getAudit().createdDate == createDate
            } else {
                assert partitions.get(0).dataUri == uri + uriResult
            }
        }
        cleanup:
        if (!error) {
            partitionApi.deletePartitions(catalogName, databaseName, tableName, [partitionName])
        }
        where:
        catalogName                     | databaseName      | tableName | partitionName | uriSuffix | uriResult | repeat | alter | error
        'embedded-hive-metastore'       | 'smoke_db'        | 'part'    | 'one=xyz'     | ''        | ''        | false  | false | null
        'embedded-hive-metastore'       | 'smoke_db'        | 'part'    | 'one=xyz'     | '/'       | ''        | false  | false | null
        'embedded-hive-metastore'       | 'smoke_db'        | 'part'    | 'one=xyz'     | ''        | ''        | true   | false | null
        'embedded-hive-metastore'       | 'smoke_db'        | 'part'    | 'one=xyz'     | ''        | ''        | true   | true  | null
        'embedded-hive-metastore'       | 'smoke_db'        | 'part'    | 'two=xyz'     | ''        | ''        | false  | false | MetacatBadRequestException.class
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | 'one=xyz'     | ''        | ''        | false  | false | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | 'one=xyz'     | '/'       | ''        | false  | false | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | 'one=xyz'     | ''        | ''        | true   | false | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | 'one=xyz'     | '/'       | ''        | true   | false | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | 'one=xyz'     | ''        | ''        | true   | true  | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | 'one=xyz'     | '/'       | ''        | true   | true  | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | 'two=xyz'     | ''        | ''        | false  | false | MetacatBadRequestException.class
        'hive-metastore'                | 'hsmoke_db'       | 'part'    | 'one=xyz'     | ''        | ''        | false  | false | null
        'hive-metastore'                | 'hsmoke_db'       | 'part'    | 'one=xyz'     | ''        | ''        | true   | false | null
        'hive-metastore'                | 'hsmoke_db'       | 'part'    | 'one=xyz'     | ''        | ''        | true   | true  | null
        'hive-metastore'                | 'hsmoke_db'       | 'part'    | 'two=xyz'     | ''        | ''        | false  | false | MetacatBadRequestException.class
        's3-mysql-db'                   | 'smoke_db'        | 'part'    | 'one=xyz'     | ''        | ''        | false  | false | null
        's3-mysql-db'                   | 'smoke_db'        | 'part'    | 'one=xyz'     | ''        | ''        | true   | true  | null
        's3-mysql-db'                   | 'smoke_db'        | 'part'    | 'one=xyz'     | ''        | ''        | true   | false | null
        's3-mysql-db'                   | 'smoke_db'        | 'part'    | 'two=xyz'     | ''        | ''        | false  | false | MetacatBadRequestException.class
        's3-mysql-db'                   | 'invalid-catalog' | 'z'       | 'one=xyz'     | ''        | ''        | false  | false | MetacatNotFoundException.class
    }

    @Unroll
    def "Test catalog failure cases"() {
        when:
        api.getDatabase('invalid', 'invalid', false, false)
        then:
        thrown(MetacatNotFoundException)
        when:
        api.createDatabase('invalid', 'invalid', new DatabaseCreateRequestDto())
        then:
        thrown(MetacatNotFoundException)
        when:
        api.updateDatabase('invalid', 'invalid', new DatabaseCreateRequestDto())
        then:
        thrown(MetacatNotFoundException)
        when:
        api.deleteDatabase('invalid', 'invalid')
        then:
        thrown(MetacatNotFoundException)
        when:
        api.getTable('invalid', 'invalid', 'invalid', false, false, false)
        then:
        thrown(MetacatNotFoundException)
        when:
        api.createTable('invalid', 'invalid', 'invalid', new TableDto())
        then:
        thrown(MetacatBadRequestException)
        when:
        api.createTable('invalid', 'invalid', 'invalid', new TableDto(name: QualifiedName.ofTable('invalid', 'invalid', 'invalid')))
        then:
        thrown(MetacatNotFoundException)
        when:
        api.updateTable('invalid', 'invalid', 'invalid', new TableDto(name: QualifiedName.ofTable('invalid', 'invalid', 'invalid')))
        then:
        thrown(MetacatNotFoundException)
        when:
        api.deleteTable('invalid', 'invalid', 'invalid')
        then:
        thrown(MetacatNotFoundException)
        when:
        partitionApi.getPartitionCount('invalid', 'invalid', 'invalid')
        then:
        thrown(MetacatNotFoundException)
        when:
        partitionApi.savePartitions('invalid', 'invalid', 'invalid', new PartitionsSaveRequestDto())
        then:
        noExceptionThrown()
        when:
        partitionApi.savePartitions('invalid', 'invalid', 'invalid', new PartitionsSaveRequestDto(partitions: [new PartitionDto()]))
        then:
        thrown(MetacatNotFoundException)
        when:
        partitionApi.deletePartitions('invalid', 'invalid', 'invalid', ['dateint=1'])
        then:
        thrown(MetacatNotFoundException)
    }

    @Unroll
    def "Test database failure cases"() {
        when:
        api.getDatabase(catalogName, 'invalid', false, true)
        then:
        thrown(MetacatNotFoundException)
        when:
        api.deleteDatabase(catalogName, 'invalid')
        api.updateDatabase(catalogName, 'invalid', new DatabaseCreateRequestDto())
        then:
        thrown(MetacatNotFoundException)
        when:
        api.deleteDatabase(catalogName, 'invalid')
        then:
        thrown(MetacatNotFoundException)
        when:
        api.getTable(catalogName, 'invalid', 'invalid', true, false, false)
        then:
        thrown(MetacatNotFoundException)
        when:
        api.createTable(catalogName, 'invalid', 'invalid', new TableDto(name: QualifiedName.ofTable('invalid', 'invalid', 'invalid')))
        then:
        thrown(MetacatNotFoundException)
        when:
        api.updateTable(catalogName, 'invalid', 'invalid', new TableDto(name: QualifiedName.ofTable('invalid', 'invalid', 'invalid')))
        then:
        thrown(MetacatNotFoundException)
        when:
        api.deleteTable(catalogName, 'invalid', 'invalid')
        then:
        // Even if the table does not exist, we ignore the error so that we can delete the definition metadata
        noExceptionThrown()
        when:
        def count = partitionApi.getPartitionCount(catalogName, 'invalid', 'invalid')
        then:
        count == 0
        when:
        partitionApi.savePartitions(catalogName, 'invalid', 'invalid', new PartitionsSaveRequestDto(partitions: [new PartitionDto()]))
        then:
        thrown(MetacatNotFoundException)
        when:
        partitionApi.deletePartitions(catalogName, 'invalid', 'invalid', ['dateint=1'])
        then:
        thrown(MetacatNotFoundException)
        when:
        api.createDatabase(catalogName, 'invalid', new DatabaseCreateRequestDto())
        api.createDatabase(catalogName, 'invalid', new DatabaseCreateRequestDto())
        then:
        thrown(MetacatAlreadyExistsException)
        when:
        api.deleteDatabase(catalogName, 'invalid')
        then:
        noExceptionThrown()
        where:
        catalogName << ['embedded-hive-metastore', 'hive-metastore', 's3-mysql-db']
    }

    @Unroll
    def "Test table failure cases"() {
        given:
        try {
            api.createDatabase(catalogName, 'invalid', new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        when:
        api.getTable(catalogName, 'invalid', 'invalid', true, false, false)
        then:
        thrown(MetacatNotFoundException)
        when:
        api.updateTable(catalogName, 'invalid', 'invalid', new TableDto(name: QualifiedName.ofTable('invalid', 'invalid', 'invalid')))
        then:
        thrown(MetacatNotFoundException)
        when:
        api.deleteTable(catalogName, 'invalid', 'invalid')
        then:
        // Even if the table does not exist, we ignore the error so that we can delete the definition metadata
        noExceptionThrown()
        when:
        def count = partitionApi.getPartitionCount(catalogName, 'invalid', 'invalid')
        then:
        count == 0
        when:
        partitionApi.savePartitions(catalogName, 'invalid', 'invalid', new PartitionsSaveRequestDto(partitions: [new PartitionDto()]))
        then:
        thrown(MetacatNotFoundException)
        when:
        partitionApi.deletePartitions(catalogName, 'invalid', 'invalid', ['dateint=1'])
        then:
        thrown(MetacatNotFoundException)
        when:
        createTable(catalogName, 'invalid', 'invalid')
        api.createTable(catalogName, 'invalid', 'invalid', new TableDto(name: QualifiedName.ofTable('invalid', 'invalid', 'invalid')))
        then:
        thrown(MetacatAlreadyExistsException)
        when:
        api.deleteTable(catalogName, 'invalid', 'invalid')
        api.deleteDatabase(catalogName, 'invalid')
        then:
        noExceptionThrown()
        where:
        catalogName << ['embedded-hive-metastore', 'hive-metastore', 's3-mysql-db']
    }

    @Unroll
    def "Test: embedded-hive-metastore get partitions for filter #filter with offset #offset and limit #limit returned #result partitions"() {
        given:
        if (cursor == 'start') {
            def uri = isLocalEnv ? 'file:/tmp/abc' : null
            createTable('embedded-hive-metastore', 'smoke_db', 'parts')
            partitionApi.savePartitions('embedded-hive-metastore', 'smoke_db', 'parts', new PartitionsSaveRequestDto(partitions: PigDataDtoProvider.getPartitions('embedded-hive-metastore', 'smoke_db', 'parts', 'one=xyz/total=1', uri, 10)))
        }
        def partitionKeys = partitionApi.getPartitionKeys('embedded-hive-metastore', 'smoke_db', 'parts', filter, null, null, offset, limit)

        expect:
        partitionKeys.size() == result
        cleanup:
        if (cursor == 'end') {
            def partitionKeysToDrop = partitionApi.getPartitionKeys('embedded-hive-metastore', 'smoke_db', 'parts', null, null, null, null, null)
            partitionApi.deletePartitions('embedded-hive-metastore', 'smoke_db', 'parts', partitionKeysToDrop)
        }
        where:
        cursor  | filter                                    | offset | limit | result
        'start' | "one=='xyz'"                              | null   | null  | 10
        ''      | "one=='xyz' and one like 'xy_'"           | null   | null  | 10
        ''      | "one=='xyz' and one like 'xy_'"           | 0      | 10    | 10
        ''      | "one=='xyz' and one like 'xy_'"           | 5      | 10    | 5
        ''      | "one=='xyz' and one like 'xy_'"           | 10     | 10    | 0
        ''      | "(one=='xyz') and one like 'xy%'"         | null   | null  | 10
        ''      | "one like 'xy%'"                          | null   | null  | 10
        ''      | "one not like 'xy%'"                      | null   | null  | 0
        ''      | "total==10"                               | null   | null  | 1
        ''      | "total<1"                                 | null   | null  | 0
        ''      | "total>1"                                 | null   | null  | 10
        ''      | "total>=10"                               | null   | null  | 10
        ''      | "total<=20"                               | null   | null  | 10
        ''      | "total between 0 and 20"                  | null   | null  | 10
        ''      | "total between 0 and 20 and one='xyz'"    | null   | null  | 10
        ''      | "total not between 0 and 20"              | null   | null  | 0
        ''      | "total between 20 and 30"                 | null   | null  | 0
        ''      | "total between 19 and 19"                 | null   | null  | 1
        ''      | "total in (19)"                           | null   | null  | 1
        ''      | "total in (18,19,20,21)"                  | null   | null  | 2
        ''      | "total not in (18,19,20,21)"              | null   | null  | 8
        ''      | "one=='xyz' and (total==11 or total==12)" | null   | null  | 2
        ''      | null                                      | 0      | 10    | 10
        ''      | null                                      | 5      | 10    | 5
        'end'   | null                                      | 10     | 10    | 0

    }

    @Unroll
    def "Load test save partitions for #catalogName/#databaseName/#tableName with partition names(#count) starting with #partitionName"() {
        when:
        def request = new PartitionsSaveRequestDto()
        def uri = isLocalEnv ? 'file:/tmp/abc' : null
        createTable(catalogName, databaseName, tableName)
        def partitions = PigDataDtoProvider.getPartitions(catalogName, databaseName, tableName, partitionName, uri, count)
        request.setPartitions(partitions)
        partitionApi.savePartitions(catalogName, databaseName, tableName, request)
        def savedPartitions = partitionApi.getPartitions(catalogName, databaseName, tableName, null, null, null, null, null, true)
        if (alter > 0) {
            for (int i = 0; i < alter; i++) {
                partitions.get(i).getSerde().setUri(partitions.get(i).getSerde().getUri() + i)
            }
            request.setAlterIfExists(true)
            partitionApi.savePartitions(catalogName, databaseName, tableName, request)
        }
        def partitionNames = savedPartitions.collect { it.name.partitionName }
        then:
        savedPartitions != null && savedPartitions.size() >= count
        cleanup:
        partitionApi.deletePartitions(catalogName, databaseName, tableName, partitionNames)
        where:
        catalogName                     | databaseName | tableName | partitionName | count | alter
        'embedded-hive-metastore'       | 'smoke_db5'  | 'part'    | 'one=xyz'     | 10    | 0
        'embedded-hive-metastore'       | 'smoke_db5'  | 'part'    | 'one=xyz'     | 10    | 10
        'embedded-hive-metastore'       | 'smoke_db5'  | 'part'    | 'one=xyz'     | 10    | 5
        'embedded-fast-hive-metastore'  | 'fsmoke_db5' | 'part'    | 'one=xyz'     | 10    | 0
        'embedded-fast-hive-metastore'  | 'fsmoke_db5' | 'part'    | 'one=xyz'     | 10    | 10
        'embedded-fast-hive-metastore'  | 'fsmoke_db5' | 'part'    | 'one=xyz'     | 10    | 5
        'hive-metastore'                | 'hsmoke_db5' | 'part'    | 'one=xyz'     | 10    | 0
        'hive-metastore'                | 'hsmoke_db5' | 'part'    | 'one=xyz'     | 10    | 10
        'hive-metastore'                | 'hsmoke_db5' | 'part'    | 'one=xyz'     | 10    | 5
    }

    @Unroll
    def "Test tag table for #catalogName/#databaseName/#tableName"() {
        when:
        createTable(catalogName, databaseName, tableName)
        tagApi.setTableTags(catalogName, databaseName, tableName, tags)
        if (repeat) {
            tagApi.setTableTags(catalogName, databaseName, tableName, tags)
        }
        def dto = api.getTable(catalogName, databaseName, tableName, false, true, true)
        then:
        metacatJson.convertValue(dto.getDefinitionMetadata().get('tags'), Set.class) == tags
        cleanup:
        tagApi.removeTableTags(catalogName, databaseName, tableName, true, [] as Set<String>)
        api.deleteTable(catalogName, databaseName, tableName)
        where:
        catalogName                     | databaseName | tableName | tags                              | repeat
        'embedded-hive-metastore'       | 'smoke_db6'  | 'part'    | ['test'] as Set<String>           | true
        'embedded-hive-metastore'       | 'smoke_db6'  | 'part'    | ['test', 'unused'] as Set<String> | false
        'embedded-fast-hive-metastore'  | 'fsmoke_db6' | 'part'    | ['test'] as Set<String>           | true
        'embedded-fast-hive-metastore'  | 'fsmoke_db6' | 'part'    | ['test', 'unused'] as Set<String> | false
        'hive-metastore'                | 'hsmoke_db6' | 'part'    | ['test'] as Set<String>           | true
        'hive-metastore'                | 'hsmoke_db6' | 'part'    | ['test', 'unused'] as Set<String> | false
        's3-mysql-db'                   | 'smoke_db6'  | 'part'    | ['test'] as Set<String>           | true
        's3-mysql-db'                   | 'smoke_db6'  | 'part'    | ['test', 'unused'] as Set<String> | false
    }

    @Unroll
    def "Delete invalid partition for #catalogName/#databaseName/#tableName"() {
        when:
        createTable(catalogName, databaseName, tableName)
        partitionApi.deletePartitions(catalogName, databaseName, tableName, partitionNames)
        then:
        thrown(MetacatNotFoundException)
        where:
        catalogName                     | databaseName | tableName | partitionNames
        'embedded-hive-metastore'       | 'smoke_db'   | 'part'    | ['one=invalid']
        'embedded-hive-metastore'       | 'smoke_db'   | 'part'    | ['one=test', 'one=invalid']
        'embedded-hive-metastore'       | 'smoke_db'   | 'part'    | ['one=test', 'one=invalid']
        'embedded-hive-metastore'       | 'smoke_db'   | 'invalid' | ['one=test', 'one=invalid']
        'embedded-fast-hive-metastore'  | 'fsmoke_db'  | 'part'    | ['one=invalid']
        'embedded-fast-hive-metastore'  | 'fsmoke_db'  | 'part'    | ['one=test', 'one=invalid']
        'embedded-fast-hive-metastore'  | 'fsmoke_db'  | 'part'    | ['one=test', 'one=invalid']
        'embedded-fast-hive-metastore'  | 'fsmoke_db'  | 'invalid' | ['one=test', 'one=invalid']
        'hive-metastore'                | 'hsmoke_db'  | 'part'    | ['one=invalid']
        'hive-metastore'                | 'hsmoke_db'  | 'part'    | ['one=test', 'one=invalid']
        'hive-metastore'                | 'hsmoke_db'  | 'part'    | ['one=test', 'one=invalid']
        'hive-metastore'                | 'hsmoke_db'  | 'invalid' | ['one=test', 'one=invalid']

    }

    @Unroll
    def "Delete definition metadata for table #name"() {
        when:
        def qName = QualifiedName.fromString(name)
        createTable(qName.getCatalogName(), qName.getDatabaseName(), qName.getTableName())
        metadataApi.deleteDefinitionMetadata(name, force)
        def list = metadataApi.getDefinitionMetadataList(null, null, null, null, null, null, name, null)
        then:
        list.isEmpty() == force
        cleanup:
        api.deleteTable(qName.getCatalogName(), qName.getDatabaseName(), qName.getTableName())
        where:
        name                                        | force
        'embedded-hive-metastore/smoke_db/dm'       | false
        'embedded-hive-metastore/smoke_db/dm'       | true
    }

    @Unroll
    @Ignore
    def "Delete definition metadata for non-existant #name"() {
        when:
        def metacatSource = Sql.newInstance('jdbc:mysql://hive-metastore-db:3306/metacat?useUnicode=true&characterEncoding=latin1&autoReconnect=true&sessionVariables=@@innodb_lock_wait_timeout=120&rewriteBatchedStatements=true', 'metacat_user','metacat_user_password', 'com.mysql.jdbc.Driver')
        metacatSource.execute("insert into definition_metadata(version,created_by,data,date_created,last_updated,last_updated_by,name) values (0,'test', '{}',now(), now(), 'test'," + name+ ")")
        metadataApi.deleteDefinitionMetadata(name, false)
        def list = metadataApi.getDefinitionMetadataList(null, null, null, null, null, null, name, null)
        then:
        list.isEmpty()
        where:
        name << ['embedded-hive-metastore/invalid/dm',
                 'embedded-hive-metastore/invalid/dm/vm',
                 'embedded-hive-metastore/invalid/dm/vm=1']
    }
}
