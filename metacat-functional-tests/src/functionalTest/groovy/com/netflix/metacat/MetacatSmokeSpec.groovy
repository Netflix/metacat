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
import com.google.common.collect.ImmutableMap
import com.netflix.metacat.client.Client
import com.netflix.metacat.client.api.MetacatV1
import com.netflix.metacat.client.api.MetadataV1
import com.netflix.metacat.client.api.ParentChildRelV1
import com.netflix.metacat.client.api.PartitionV1
import com.netflix.metacat.client.api.TagV1
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.exception.MetacatBadRequestException
import com.netflix.metacat.common.exception.MetacatNotFoundException
import com.netflix.metacat.common.exception.MetacatNotSupportedException
import com.netflix.metacat.common.exception.MetacatPreconditionFailedException
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataConstants
import com.netflix.metacat.testdata.provider.PigDataDtoProvider
import feign.Logger
import feign.RetryableException
import feign.Retryer
import groovy.sql.Sql
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.Assertions
import org.skyscreamer.jsonassert.JSONAssert
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.stream.IntStream

/**
 * MetacatSmokeSpec.
 * @author amajumdar
 * @author zhenl
 * @since 1.0.0
 */
class MetacatSmokeSpec extends Specification {
    public static MetacatV1 api
    public static MetacatV1 hiveContextApi
    public static PartitionV1 partitionApi
    public static MetadataV1 metadataApi
    public static TagV1 tagApi
    public static ParentChildRelV1 parentChildRelV1
    public static MetacatJson metacatJson = new MetacatJsonLocator()

    def setupSpec() {
        String url = "http://localhost:${System.properties['metacat_http_port']}"
        assert url, 'Required system property "metacat_url" is not set'

        def client = Client.builder()
            .withHost(url)
            .withUserName('metacat-test')
            .withClientAppName('metacat-test')
            .withLogLevel(Logger.Level.NONE)
            .withRetryer(Retryer.NEVER_RETRY)
            .build()
        def hiveClient = Client.builder()
            .withHost(url)
            .withLogLevel(Logger.Level.NONE)
            .withDataTypeContext('hive')
            .withUserName('metacat-test')
            .withClientAppName('metacat-test')
            .withRetryer(Retryer.NEVER_RETRY)
            .build()
        hiveContextApi = hiveClient.api
        api = client.api
        partitionApi = client.partitionApi
        tagApi = client.tagApi
        metadataApi = client.metadataApi
        parentChildRelV1 = client.parentChildRelApi
    }

    @Shared
    boolean isLocalEnv = Boolean.valueOf(System.getProperty("local", "true"))

    static createTable(String catalogName, String databaseName, String tableName) {
        createTable(catalogName, databaseName, tableName, null)
    }

    static createTable(String catalogName, String databaseName, String tableName, String externalValue) {
        createTable(catalogName, databaseName, tableName, ['EXTERNAL':externalValue])
    }

    static createTable(String catalogName, String databaseName, String tableName, Map<String, String> metadata) {
        def catalog = api.getCatalog(catalogName)
        if (!catalog.databases.contains(databaseName)) {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
            Thread.sleep(5000)
        }
        def database = api.getDatabase(catalogName, databaseName, false, true)
        def owner = 'amajumdar'
        def uri = null
        if (Boolean.valueOf(System.getProperty("local", "true"))) {
            uri = String.format('file:/tmp/%s/%s', databaseName, tableName)
        }
        if (!database.getTables().contains(tableName)) {
            def newTable
            if ('part' == tableName || 'fsmoke_db__part__audit_12345' == tableName) {
                newTable = PigDataDtoProvider.getPartTable(catalogName, databaseName, tableName, owner, uri)
            } else if ('parts' == tableName  ) {
                newTable = PigDataDtoProvider.getPartsTable(catalogName, databaseName, owner, uri)
            } else if ('metacat_all_types' == tableName) {
                newTable = PigDataDtoProvider.getMetacatAllTypesTable(catalogName, databaseName, owner, uri)
            } else if ('part_hyphen' == tableName  ) {
                newTable = PigDataDtoProvider.getPartHyphenTable(catalogName, databaseName, tableName, owner, uri)
            } else {
                newTable = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, owner, uri)
            }
            if (metadata != null) {
                newTable.getMetadata() == null? newTable.setMetadata(metadata): newTable.getMetadata().putAll(metadata)
            }

            if (newTable.getSerde() == null) {
                newTable.setSerde(new StorageDto(owner: owner))
            }

            if (newTable.getSerde().getOwner() == null) {
                newTable.getSerde().setOwner(owner)
            }
            api.createTable(catalogName, databaseName, tableName, newTable)
        }
    }

    static void initializeParentChildRelDefinitionMetadata(TableDto tableDto,
                                                      String parent,
                                                      String parent_uuid,
                                                      String child_uuid) {
        def mapper = new ObjectMapper()
        def innerNode = mapper.createObjectNode()
        innerNode.put(ParentChildRelMetadataConstants.PARENT_NAME, parent)
        innerNode.put(ParentChildRelMetadataConstants.PARENT_UUID, parent_uuid)
        innerNode.put(ParentChildRelMetadataConstants.CHILD_UUID, child_uuid)

        tableDto.definitionMetadata.put(ParentChildRelMetadataConstants.PARENT_CHILD_RELINFO, innerNode)
    }

    def createAllTypesTable() {
        when:
        createTable('polaris-metastore', 'smoke_db', 'metacat_all_types')
        def table = api.getTable('polaris-metastore', 'smoke_db', 'metacat_all_types', true, true, true)
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
        catalogNames.contains('s3-mysql-db')
        catalogNames.contains('polaris-metastore')
    }

    def testCreateCatalog() {
        when:
        api.createCatalog(new CreateCatalogDto())
        then:
        thrown(MetacatNotSupportedException)
    }

    @Unroll
    def "Test create/get table with nested fields with upper case"() {
        given:
        def catalogName = 'polaris-metastore'
        def databaseName = 'iceberg_db'
        def tableName = 'iceberg_table_with_upper_case_nested_fields'
        def uri = isLocalEnv ? String.format('file:/tmp/data/') : null
        def tableDto = new TableDto(
            name: QualifiedName.ofTable(catalogName, databaseName, tableName),
            serde: new StorageDto(
                owner: 'metacat-test',
                inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                parameters: [
                    'serialization.format': '1'
                ],
                uri: uri
            ),
            definitionMetadata: null,
            dataMetadata: null,
            fields: [
                new FieldDto(
                    comment: null,
                    name: "dateint",
                    pos: 0,
                    type: "long",
                    partition_key: true,
                ),
                new FieldDto(
                    comment: null,
                    name: "info",
                    pos: 1,
                    partition_key: false,
                    type: "(name: chararray, address: (NAME: chararray), nestedArray: {(FIELD1: chararray, field2: chararray)})",
                )
            ]
        )

        def metadataLocation = String.format('/tmp/data/metadata/00000-0b60cc39-438f-413e-96c2-2694d7926529.metadata.json')

        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)
        when:
        try {api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        api.createTable(catalogName, databaseName, tableName, tableDto)
        def tableDTO = api.getTable(catalogName, databaseName, tableName, true, true, true)

        then:
        noExceptionThrown()
        tableDTO.metadata.get("metadata_location").equals(metadataLocation)
        tableDTO.getFields().size() == 2
        def nestedFieldDto = tableDTO.getFields().find { it.name == "info" }
        // assert that the type field also keeps the name fidelity
        assert nestedFieldDto.type == "(name: chararray,address: (NAME: chararray),nestedArray: {(FIELD1: chararray,field2: chararray)})" : "The type differ from the expected. They are: $nestedFieldDto.type"

        // assert that the json representation keeps the name fidelity
        def expectedJsonString = """
            {
                "type": "row",
                "fields": [
                    {
                        "name": "name",
                        "type": "chararray"
                    },
                    {
                        "name": "address",
                        "type": {
                            "type": "row",
                            "fields": [
                                {
                                    "name": "NAME",
                                    "type": "chararray"
                                }
                            ]
                        }
                    },
                    {
                        "name": "nestedArray",
                        "type": {
                            "type": "array",
                            "elementType": {
                                "type": "row",
                                "fields": [
                                    {
                                        "name": "FIELD1",
                                        "type": "chararray"
                                    },
                                    {
                                        "name": "field2",
                                        "type": "chararray"
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
            """

        JSONAssert.assertEquals(nestedFieldDto.jsonType.toString(), expectedJsonString, false)
        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)
    }

    @Unroll
    def "Test create database for #catalogName/#databaseName"() {
        given:
        def definitionMetadata = metacatJson.emptyObjectNode().set("owner", metacatJson.emptyObjectNode())
        expect:
        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto(definitionMetadata: definitionMetadata, uri:uri))
            error == null
        } catch (Exception e) {
            e.class == error
        }
        if (!error) {
            def database = api.getDatabase(catalogName, databaseName, true, true)
            assert database != null && database.name.databaseName == databaseName
            assert database.definitionMetadata != null && database.definitionMetadata == definitionMetadata
            assert uri == null || uri == database.uri
        }
        cleanup:
        if (!error) {
            api.deleteDatabase(catalogName, databaseName)
        }
        where:
        catalogName                     | databaseName | uri                                                  | error
        'polaris-metastore'                | 'smoke_db0'  | 'file:/tmp/hive-metastore/smoke_db00'                | null
        'polaris-metastore'                | 'fsmoke_db0' | 'file:/tmp/hive-metastore/fsmoke_db00'               | null
        'polaris-metastore'                | 'shard1'     | null                                                 | null
        'polaris-metastore'                | 'shard1'     | null                                                 | null
        'polaris-metastore'                | 'hsmoke_db0' | 'file:/tmp/hive-metastore/hsmoke_db00'               | null
        's3-mysql-db'                   | 'smoke_db0'  | null                                                 | null
        'invalid-catalog'               | 'smoke_db0'  | null                                                 | MetacatNotFoundException.class
    }

    @Unroll
    def "Test update database for #catalogName/#databaseName"() {
        given:
        def metadata = ['a':'1']
        def definitionMetadata = metacatJson.emptyObjectNode().set("owner", metacatJson.emptyObjectNode())
        api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto(definitionMetadata: definitionMetadata))
        api.updateDatabase(catalogName, databaseName, new DatabaseCreateRequestDto(metadata: metadata, uri: uri))
        def database = api.getDatabase(catalogName, databaseName, true, true)
        expect:
        database != null
        database.definitionMetadata == definitionMetadata
        database.metadata == metadata
        uri == null || uri == database.uri
        cleanup:
        api.deleteDatabase(catalogName, databaseName)
        where:
        catalogName                     | databaseName | uri
        'polaris-metastore'                | 'smoke_db0'  | null
        'polaris-metastore'                | 'fsmoke_db0' | 'file:/tmp/hive-metastore/fsmoke_db00'
        'polaris-metastore'                | 'shard1'     | null
        'polaris-metastore'                | 'hsmoke_db0' | null
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
            assert api.doesTableExist(catalogName, databaseName, tableName)
            def table = api.getTable(catalogName, databaseName, tableName, true, true, false)
            assert table != null && table.name.tableName == tableName
            assert table.getDefinitionMetadata() != null
            if (!catalogName.startsWith('polaris')) {
                assert table.getSerde().getUri() == updatedUri
                //TODO: assert table.getSerde().getParameters().size() == 0
                assert table.getSerde().getSerdeInfoParameters().size() >= 1
                assert table.getSerde().getSerdeInfoParameters().get('serialization.format') == '1'
            }
            table = api.getTable(catalogName, databaseName, tableName, true, true, false)
            assert table.getFields().get(0).type == 'chararray'
            assert table.getFields().find{it.name=='field4'}.jsonType ==
                metacatJson.parseJsonObject('{"elementType": {"type": "row","fields": [{"name": "version","type": "chararray"},{"name": "ts","type": "long"}]},"type": "array"}')
            // from the cache
            def cachedTable = hiveContextApi.getTable(catalogName, databaseName, tableName, true, true, false)
            assert cachedTable.getFields().get(0).type == 'string'
        }
        cleanup:
        if (!error) {
            api.deleteTable(catalogName, databaseName, tableName)
        }
        where:
        catalogName                     | databaseName | tableName           | setUri | setNull | error
        'polaris-metastore'                | 'smoke_db1'  | 'test_create_table' | true   | false   | null
        'polaris-metastore'                | 'smoke_db1'  | 'test_create_table' | false  | false   | null
        'polaris-metastore'                | 'smoke_db1'  | 'test_create_table1'| false  | true    | null
        'polaris-metastore'                | 'fsmoke_db1' | 'test_create_table' | true   | false   | null
        'polaris-metastore'                | 'fsmoke_db1' | 'test_create_table' | false  | false   | null
        'polaris-metastore'                | 'fsmoke_db1' | 'test_create_table1'| false  | true    | null
        'polaris-metastore'                | 'fsmoke_db1' | 'test.create_table1'| false  | true    | InvalidMetaException.class
        'polaris-metastore'                | 'shard'      | 'test_create_table' | true   | false   | null
        'polaris-metastore'                | 'shard'      | 'test_create_table' | false  | false   | null
        'polaris-metastore'                | 'shard'      | 'test_create_table1'| false  | true    | null
        'polaris-metastore'                | 'shard'      | 'test.create_table1'| false  | true    | InvalidMetaException.class
        'polaris-metastore'                | 'hsmoke_db1' | 'test_create_table' | true   | false   | null
        'polaris-metastore'                | 'hsmoke_db1' | 'test_create_table' | false  | false   | null
        'polaris-metastore'                | 'hsmoke_db1' | 'test_create_table1'| false  | true    | null
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table' | true   | false   | null
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table.definition' | true   | false   | null   //verify the table name can have . character
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table.knp' | true   | false   | null   //verify the table name can have knp extension
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table.mp3' | true   | false   | null   //verify the table name can have knp extension
        'invalid-catalog'               | 'smoke_db1'  | 'z'                 | true   | false   | MetacatNotFoundException.class
    }

    def testCreateTableWithOwner() {
        given:
        def catalogName = 'polaris-metastore'
        def databaseName = 'fsmoke_db1_owner'
        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        when:
        def tableName ='test_create_table'
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'amajumdar', uri)
        api.createTable(catalogName, databaseName, tableName, tableDto)
        then:
        api.getTable(catalogName, databaseName, tableName, false, true, false).definitionMetadata.at('/owner/userId').textValue() == 'amajumdar'
        api.deleteTable(catalogName, databaseName, tableName)
        when:
        tableName ='test_create_table1'
        uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, null, uri)
        api.createTable(catalogName, databaseName, tableName, tableDto)
        then:
        assert api.doesTableExist(catalogName, databaseName, tableName)
        api.getTable(catalogName, databaseName, tableName, false, true, false).definitionMetadata.at('/owner/userId').textValue() == 'metacat-test'
        api.deleteTable(catalogName, databaseName, tableName)
        when:
        tableName ='test_create_table2'
        uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'amajumdar', uri)
        tableDto.getSerde().setOwner('amajumdar1')
        api.createTable(catalogName, databaseName, tableName, tableDto)
        then:
        api.getTable(catalogName, databaseName, tableName, false, true, false).definitionMetadata.at('/owner/userId').textValue() == 'amajumdar'
        api.deleteTable(catalogName, databaseName, tableName)
    }

    def "Test create/delete table for #catalogName/#databaseName/#tableName not delete usermetadata"() {
        given:
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        expect:
        try {
            try {api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
            } catch (Exception ignored) {
            }

            def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'amajumdar', uri)
            if (!setUri) {
                tableDto.getSerde().setUri(null)
            }
            if(setNull) {
                tableDto.getSerde().setInputFormat(null)
                tableDto.getSerde().setOutputFormat(null)
                tableDto.getSerde().setSerializationLib(null)
            }
            tableDto.getDefinitionMetadata().set("hive", metacatJson.parseJsonObject('{"objectField": {}}'))
            api.createTable(catalogName, databaseName, tableName, tableDto)
            error == null
        } catch (Exception e) {
            e.class == error
        }

        if (!error) {
            api.deleteTable(catalogName, databaseName, tableName)
            //create table again
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
            def table2 = api.getTable(catalogName, databaseName, tableName, true, true, false)
            assert table2.getDefinitionMetadata() != null
            assert table2.getDefinitionMetadata().get("hive").get("objectField") !=null
            assert table2.getDefinitionMetadata().get("hive").size() == 2

        }
        cleanup:
        if (!error) {
            api.deleteTable(catalogName, databaseName, tableName)
        }

        where:
        catalogName                     | databaseName | tableName           | setUri | setNull | error
        'polaris-metastore'  | 'fsmoke_db2' | 'test_create_table' | true   | false   | null
    }

    def "Test create/delete database/table for #catalogName/#databaseName/#tableName with ACL"() {
        given:
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        when:
        try {api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }

        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'amajumdar', uri)
        tableDto.getSerde().setUri(null)
        tableDto.getDefinitionMetadata().set("hive", metacatJson.parseJsonObject('{"objectField": {}}'))
        api.createTable(catalogName, databaseName, tableName, tableDto)

        then:
        thrown(MetacatUnAuthorizedException)

        when:
        api.deleteDatabase(catalogName, databaseName)
        then:
        thrown(MetacatUnAuthorizedException)

        where:
        catalogName                     | databaseName | tableName
        'polaris-metastore'                | 'fsmoke_acl' | 'test_create_table'
    }

    def "Test get table names"() {
        given:
        def catalogName = 'polaris-metastore'
        def databaseName = 'fsmoke_db_names'
        def database1Name = 'fsmoke_db1_names'
        def database2Name = 'fsmoke_db2_names'
        def tableName = 'fsmoke_table_names'
        def ownerFilter = 'hive_filter_field_owner__= "amajumdar"'
        IntStream.range(0,15).forEach{ i -> createTable(catalogName, databaseName, tableName + i)}
        IntStream.range(0,25).forEach{ i -> createTable(catalogName, database1Name, tableName + i)}
        IntStream.range(0,10).forEach{ i -> createTable(catalogName, database2Name, tableName + i, ['type':'ice'])}
        // Sleep to allow CockroachDB's follower_read_timestamp() to see newly created databases
        Thread.sleep(5000)
        when:
        api.getTableNames(catalogName, null, 10)
        then:
        thrown(MetacatBadRequestException)
        when:
        def result = api.getTableNames(catalogName, databaseName, ownerFilter, 10)
        then:
        result.size() == 10
        when:
        result = api.getTableNames(catalogName, databaseName, ownerFilter, 100)
        then:
        result.size() == 15
        cleanup:
        IntStream.range(0,15).forEach{ i -> api.deleteTable(catalogName, databaseName, tableName + i)}
        IntStream.range(0,25).forEach{ i -> api.deleteTable(catalogName, database1Name, tableName + i)}
        IntStream.range(0,10).forEach{ i -> api.deleteTable(catalogName, database2Name, tableName + i)}
    }

    def "Test materialized common view create/drop"() {
        given:
        def databaseName = 'iceberg_db'
        def storageTableName = 'st_iceberg_table'
        def commonViewName = 'test_common_view'
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, storageTableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, storageTableName, 'test', uri)
        tableDto.setFields([])
        def metadataLocation = '/tmp/data/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json'
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)
        def viewUri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, commonViewName) : null
        def commonViewDto = PigDataDtoProvider.getTable(catalogName, databaseName, commonViewName, 'test', viewUri)
        commonViewDto.setFields([])
        def viewMetadataLocation = '/tmp/data/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json'
        def viewMetadata = [common_view: 'true', metadata_location: viewMetadataLocation, storage_table: 'st_iceberg_table']
        commonViewDto.setMetadata(viewMetadata)
        when:
        def catalog = api.getCatalog(catalogName)
        if (!catalog.databases.contains(databaseName)) {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
            Thread.sleep(5000)
        }
        api.createTable(catalogName, databaseName, storageTableName, tableDto)
        then:
        noExceptionThrown()
        when:
        api.createTable(catalogName, databaseName, commonViewName, commonViewDto)
        then:
        noExceptionThrown()
        when:
        api.deleteTable(catalogName, databaseName, commonViewName)
        api.getTable(catalogName, databaseName, storageTableName, true, false, false)
        then:
        thrown(MetacatNotFoundException)
        where:
        catalogName << ['polaris-metastore']
    }

    @Unroll
    def "Test create/update iceberg table in #catalogName"() {
        given:
        def databaseName = 'iceberg_db'
        def tableName = 'iceberg_table'
        def renamedTableName = 'iceberg_table_rename'
        def icebergManifestFileName = '/metacat-test-cluster/etc-metacat/data/icebergManifest.json'
        def metadataFileName = '/metacat-test-cluster/etc-metacat/data/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json'
        def curWorkingDir = new File("").getAbsolutePath()
        def icebergManifestFile = new File(curWorkingDir + icebergManifestFileName)
        def metadataFile = new File(curWorkingDir + metadataFileName)
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'test', uri)
        tableDto.setFields([])
        def metadataLocation = 'file:/tmp/data/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json'
        def icebergMetadataLocation = 'file:/tmp/data/icebergManifest.json'
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)
        when:
        // For Polaris, all tables are Iceberg tables, so we skip the "non-Iceberg to Iceberg update fails" test
        // and directly create an Iceberg table
        api.createTable(catalogName, databaseName, tableName, tableDto)
        def metadataLocation1 = 'file:/tmp/data/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json'
        def metadata1 = [table_type: 'ICEBERG', metadata_location: metadataLocation1, previous_metadata_location: metadataLocation]
        def updatedUri = tableDto.getDataUri() + 'updated'
        tableDto.getMetadata().putAll(metadata1)
        tableDto.getSerde().setUri(updatedUri)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        noExceptionThrown()
        when:
        tagApi.setTableTags(catalogName, databaseName, tableName, ['do_not_drop','iceberg_migration_do_not_modify'] as Set)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        thrown(RetryableException)
        when:
        tagApi.setTableTags(catalogName, databaseName, tableName, ['do_not_drop'] as Set)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        noExceptionThrown()
        when:
        tagApi.removeTableTags(catalogName, databaseName, tableName, true, [] as Set)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        noExceptionThrown()
        when:
        def tableMetadataOnly = api.getTable(catalogName, databaseName, tableName, true, false, false, false, true)
        then:
        tableMetadataOnly.getFields().size() == 0
        tableMetadataOnly.getMetadata().get('metadata_location') != null
        when:
        metadataFile.renameTo(metadataFile.getAbsolutePath() + '1')
        Thread.sleep(1000) // Sleep to ensure the writes have time to settle
        api.getTable(catalogName, databaseName, tableName, true, false, false)
        then:
        thrown(MetacatBadRequestException)
        new File(metadataFile.getAbsolutePath() + '1').renameTo(metadataFile)
        Thread.sleep(1000) // Sleep to ensure the writes have time to settle
        when:
        def updatedTable = api.getTable(catalogName, databaseName, tableName, true, false, false)
        then:
        noExceptionThrown()
        api.doesTableExist(catalogName, databaseName, tableName)
        updatedTable.getMetadata().get('metadata_location') == metadataLocation1
        updatedTable != null
        if (catalogName == 'polaris-metastore') {
            updatedTable.getDataUri() == updatedUri
            updatedTable.getSerde().getInputFormat() == 'org.apache.hadoop.mapred.TextInputFormat'
        }
        when:
        def metadataLocation2 = 'file:/tmp/data/metadata/00002-2d6c1951-31d5-4bea-8edd-e35746b172f3.metadata.json'
        def metadata2 = [table_type: 'ICEBERG', metadata_location: metadataLocation2, previous_metadata_location: metadataLocation1, 'partition_spec': 'invalid']
        tableDto.getMetadata().putAll(metadata2)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        updatedTable = api.getTable(catalogName, databaseName, tableName, true, false, false)
        then:
        noExceptionThrown()
        updatedTable.getMetadata().get('metadata_location') == metadataLocation2
        updatedTable.getMetadata().get('partition_spec') != 'invalid'
        !updatedTable.getMetadata().containsKey('metadata_content')
        updatedTable.getFields().get(1).getSource_type().equals('string')
        when:
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        def tableWithDetails = api.getTable(catalogName, databaseName, tableName, true, false, false, true)
        then:
        tableWithDetails.getMetadata().get('metadata_content') != null
        when:
        api.deleteTable(catalogName, databaseName, renamedTableName)
        api.renameTable(catalogName, databaseName, tableName, renamedTableName)
        def t = api.getTable(catalogName, databaseName, renamedTableName, true, false, false)
        t.getMetadata().put('previous_metadata_location', metadataLocation1)
        api.updateTable(catalogName, databaseName, renamedTableName, t)
        then:
        noExceptionThrown()
        api.getTable(catalogName, databaseName, renamedTableName, true, false, false) != null
        when:
        api.renameTable(catalogName, databaseName, renamedTableName, tableName)
        then:
        noExceptionThrown()
        api.getTable(catalogName, databaseName, tableName, true, false, false) != null
        when:
        tableDto.getMetadata().putAll(metadata1)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        thrown(MetacatPreconditionFailedException)
        when:
        metadata1.put('previous_metadata_location', 'file:/tmp/data/metadata/filenotfound')
        tableDto.getMetadata().putAll(metadata1)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        thrown(MetacatBadRequestException)
        when:
        // Failure to get table after a successful update shouldn't fail
        def updatedInvalidMetadata = [table_type: 'ICEBERG', metadata_location: icebergMetadataLocation, previous_metadata_location: updatedTable.getMetadata().get('metadata_location')]
        tableDto.getMetadata().putAll(updatedInvalidMetadata)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        noExceptionThrown()
        when:
        // delete table on an invalid metadata location shouldn't fail
        api.deleteTable(catalogName, databaseName, tableName)
        then:
        !api.doesTableExist(catalogName, databaseName, tableName)
        noExceptionThrown()
        cleanup:
        FileUtils.deleteQuietly(icebergManifestFile)
        where:
        catalogName << ['polaris-metastore']
    }

    @Unroll
    def "Test get iceberg table and partitions"() {
        given:
        def catalogName = 'polaris-metastore'
        def databaseName = 'iceberg_db'
        def tableName = 'iceberg_table_6'
        def uri = isLocalEnv ? String.format('file:/tmp/data/') : null
        def tableDto = new TableDto(
            name: QualifiedName.ofTable(catalogName, databaseName, tableName),
            serde: new StorageDto(
                owner: 'metacat-test',
                inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                parameters: [
                    'serialization.format': '1'
                ],
                uri: uri
            ),
            definitionMetadata: null,
            dataMetadata: null,
            fields: [
                new FieldDto(
                    comment: 'added 1st - partition key',
                    name: 'field1',
                    pos: 0,
                    type: 'boolean',
                    partition_key: true
                )
            ]
        )

        def metadataLocation = String.format('/tmp/data/metadata/00002-2d6c1951-31d5-4bea-8edd-e35746b172f3.metadata.json')

        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)
        when:
        try {api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        api.createTable(catalogName, databaseName, tableName, tableDto)
        def tableDTO = api.getTable(catalogName, databaseName, tableName, true, true, true)
        def parts = partitionApi.getPartitions(catalogName, databaseName, tableName, null, null, null, null, null, true)
        def partkeys = partitionApi.getPartitionKeys(catalogName, databaseName, tableName, null, null, null, null, null)

        then:
        noExceptionThrown()
        tableDTO.metadata.get("metadata_location").equals(metadataLocation)
        tableDTO.getFields().size() == 3
        tableDTO.getFields().get(0).getComment() != null
        parts.size() == 2
        parts.get(0).dataMetadata != null
        partkeys.size() == 2

        when:
        // Polaris connector now supports getPartitionCount
        def partitionCount = partitionApi.getPartitionCount(catalogName, databaseName, tableName)
        then:
        noExceptionThrown()
        partitionCount == 2

        when:
        partitionApi.getPartitionUris(catalogName, databaseName, tableName, null, null, null, null, null)
        then:
        noExceptionThrown()

        when:
        partitionApi.deletePartitions(catalogName, databaseName, tableName, ['field1=true'])
        then:
        thrown(MetacatNotSupportedException)

        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)
    }

    @Unroll
    def "Test ignore void transform as partition fields"() {
        given:
        def catalogName = 'polaris-metastore'
        def databaseName = 'iceberg_db'
        def tableName = 'iceberg_table_6'
        def uri = isLocalEnv ? String.format('file:/tmp/data/') : null
        def tableDto = new TableDto(
            name: QualifiedName.ofTable(catalogName, databaseName, tableName),
            serde: new StorageDto(
                owner: 'metacat-test',
                inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                parameters: [
                    'serialization.format': '1'
                ],
                uri: uri
            ),
            definitionMetadata: null,
            dataMetadata: null,
            fields: null
        )
        def metadataLocation = String.format('/tmp/data/metadata/00001-ba4b775c-7b1a-4a6f-aec0-03d3c851088c.metadata.json')
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)
        when:
        try {api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        api.createTable(catalogName, databaseName, tableName, tableDto)
        def tableDTO = api.getTable(catalogName, databaseName, tableName, true, true, true)
        then:
        tableDTO.getFields().size() == 4
        tableDTO.getPartition_keys().size() == 1
        tableDTO.getPartition_keys()[0] == "field1"
        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)
    }

    @Unroll
    def "Test get partitions from iceberg table using #filter"() {
        def catalogName = 'polaris-metastore'
        def databaseName = 'iceberg_db'
        def tableName = 'iceberg_table_6'
        def tableDto = new TableDto(
            name: QualifiedName.ofTable(catalogName, databaseName, tableName),
            metadata: [table_type: 'ICEBERG',
                       metadata_location: String.format('/tmp/data/metadata/00002-2d6c1951-31d5-4bea-8edd-e35746b172f3.metadata.json')]
        )

        given:
        if ( run == "start" ) {
            try {api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
            } catch (Exception ignored) {
            }
            api.createTable(catalogName, databaseName, tableName, tableDto)

        }
        def parts = partitionApi.getPartitions(catalogName, databaseName, tableName, filter, null, null, null, null, true)
        def partkeys = partitionApi.getPartitionKeys(catalogName, databaseName, tableName, filter, null, null, null, null)

        expect:
        parts.size() == result
        partkeys.size() == result

        cleanup:
        if ( run == "end" ) {
            api.deleteTable(catalogName, databaseName, tableName)
        }

        where:
        run     | filter                                         | result
        "start" | "dateint==20180503"                            | 1
        null    | "dateint!=20180503"                            | 1
        null    | "dateint==20180503 or dateint==20180403"       | 2
        null    | "dateint!=20180503 and dateint!=20180403"      | 0
        null    | "dateint<=20180503 and dateint>=20180403"      | 2
        null    | "dateint>20180503"                             | 0
        null    | "dateint>=20180503"                            | 1
        null    | "dateint<=20180503"                            | 2
        null    | "dateint between 20180403 and 20180503"        | 2
        null    | "dateint not between 20180403 and 20180503"    | 0
        "end"   | "dateint>=20180403"                            | 2


    }

    @Unroll
    def "Test iceberg metadata injection for branches/tags info in #catalogName"() {
        given:
        def databaseName = 'iceberg_metadata_db'
        def tableName = 'iceberg_metadata_table'
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'test', uri)
        tableDto.setFields([])
        def metadataLocation = '/tmp/data/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json'
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)

        when:
        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }

        api.createTable(catalogName, databaseName, tableName, tableDto)
        def retrievedTable = api.getTable(catalogName, databaseName, tableName, true, true, false)

        then:
        noExceptionThrown()
        retrievedTable != null
        retrievedTable.metadata != null
        retrievedTable.metadata.containsKey('table_type')
        retrievedTable.metadata.get('table_type') == 'ICEBERG'
        retrievedTable.metadata.containsKey('metadata_location')
        retrievedTable.metadata.get('metadata_location') == metadataLocation

        // Verify that the iceberg.has.non.main.branches and iceberg.has.tags flags are populated correctly
        retrievedTable.metadata.containsKey('iceberg.has.non.main.branches')
        retrievedTable.metadata.containsKey('iceberg.has.tags')
        // Basic test tables without additional non-main branches/tags should return "false" for both
        // This verifies both the injection mechanism AND the logic correctness for simple tables
        retrievedTable.metadata.get('iceberg.has.non.main.branches') == 'false'
        retrievedTable.metadata.get('iceberg.has.tags') == 'false'

        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)

        where:
        catalogName << ['polaris-metastore']
    }

    @Unroll
    def "Test iceberg metadata injection with real branches and tags in #catalogName"() {
        given:
        def databaseName = 'iceberg_branches_tags_db'
        def tableName = 'iceberg_branches_tags_table'
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'test', uri)
        tableDto.setFields([])

        def metadataLocation = '/tmp/data/metadata/00003-with-branches-and-tags.metadata.json'
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)

        when:
        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }

        api.createTable(catalogName, databaseName, tableName, tableDto)
        def retrievedTable = api.getTable(catalogName, databaseName, tableName, true, true, false)

        then:
        noExceptionThrown()
        retrievedTable != null
        retrievedTable.metadata != null
        retrievedTable.metadata.containsKey('table_type')
        retrievedTable.metadata.get('table_type') == 'ICEBERG'
        retrievedTable.metadata.containsKey('metadata_location')
        retrievedTable.metadata.get('metadata_location') == metadataLocation

        // Verify that the metadata flags correctly detect the branches and tags from the real metadata file
        retrievedTable.metadata.containsKey('iceberg.has.non.main.branches')
        retrievedTable.metadata.containsKey('iceberg.has.tags')

        // The metadata file has 3 branches (main, feature-branch, experimental) and 3 tags (v1.0.0, v2.0.0, release-2024-01)
        // So both flags should be "true"
        retrievedTable.metadata.get('iceberg.has.non.main.branches') == 'true'
        retrievedTable.metadata.get('iceberg.has.tags') == 'true'

        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)

        where:
        catalogName << ['polaris-metastore']
    }

    @Unroll
    def "Test iceberg metadata injection with main branch only in #catalogName"() {
        given:
        def databaseName = 'iceberg_main_only_db'
        def tableName = 'iceberg_main_only_table'
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'test', uri)
        tableDto.setFields([])

        // Use the metadata file that contains only the main branch (no additional branches or tags)
        def metadataLocation = '/tmp/data/metadata/00004-main-branch-only.metadata.json'
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)

        when:
        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }

        api.createTable(catalogName, databaseName, tableName, tableDto)
        def retrievedTable = api.getTable(catalogName, databaseName, tableName, true, true, false)

        then:
        noExceptionThrown()
        retrievedTable != null
        retrievedTable.metadata != null
        retrievedTable.metadata.containsKey('table_type')
        retrievedTable.metadata.get('table_type') == 'ICEBERG'
        retrievedTable.metadata.containsKey('metadata_location')
        retrievedTable.metadata.get('metadata_location') == metadataLocation

        // Verify that the metadata flags correctly detect no additional branches or tags
        retrievedTable.metadata.containsKey('iceberg.has.non.main.branches')
        retrievedTable.metadata.containsKey('iceberg.has.tags')

        // The metadata file has only 1 branch (main) and 0 tags
        // So both flags should be "false"
        retrievedTable.metadata.get('iceberg.has.non.main.branches') == 'false'
        retrievedTable.metadata.get('iceberg.has.tags') == 'false'

        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)

        where:
        catalogName << ['polaris-metastore']
    }

    @Unroll
    def "Test iceberg metadata injection with table created by Iceberg client < 0.14.1 (no refs section) in #catalogName"() {
        given:
        def databaseName = 'iceberg_pre_0141_client_db'
        def tableName = 'iceberg_pre_0141_client_table'
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'test', uri)
        tableDto.setFields([])

        // Use REAL metadata file created with Iceberg client < 0.14.1 (Dec 2021, before refs support)
        // Note: This is v1 format but could be any format - client version determines refs support
        def metadataLocation = '/tmp/data/metadata/00005-old-client-no-refs.metadata.json'
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)

        when:
        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }

        api.createTable(catalogName, databaseName, tableName, tableDto)
        def retrievedTable = api.getTable(catalogName, databaseName, tableName, true, true, false)

        then:
        noExceptionThrown()
        retrievedTable != null
        retrievedTable.metadata != null
        retrievedTable.metadata.containsKey('table_type')
        retrievedTable.metadata.get('table_type') == 'ICEBERG'
        retrievedTable.metadata.containsKey('metadata_location')
        retrievedTable.metadata.get('metadata_location') == metadataLocation

        // Verify that tables created with Iceberg < 0.14.1 are handled correctly
        retrievedTable.metadata.containsKey('iceberg.has.non.main.branches')
        retrievedTable.metadata.containsKey('iceberg.has.tags')

        // CRITICAL: JSON metadata has no refs section, but Iceberg runtime auto-creates "main" branch
        // Tables created with Iceberg < 0.14.1: JSON has no refs, but runtime has main branch
        // - hasNonMainBranches() == false (only auto-created main branch, size=1, so 1 > 1 is false)
        // - hasTags() == false (no tags in auto-created refs)
        // This ensures backward compatibility with tables created before branches/tags support
        retrievedTable.metadata.get('iceberg.has.non.main.branches') == 'false'
        retrievedTable.metadata.get('iceberg.has.tags') == 'false'

        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)

        where:
        catalogName << ['polaris-metastore']
    }

    @Unroll
    def "Test iceberg v1 format with branches/tags (created with Iceberg client >= 0.14.1) in #catalogName"() {
        given:
        def databaseName = 'iceberg_v1_with_refs_db'
        def tableName = 'iceberg_v1_with_refs_table'
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'test', uri)
        tableDto.setFields([])

        // Use v1 format metadata file with branches/tags (created with Iceberg client >= 0.14.1)
        // This proves that format version != client capability - v1 can have refs!
        def metadataLocation = '/tmp/data/metadata/00006-v1-with-branches-tags.metadata.json'
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)

        when:
        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }

        api.createTable(catalogName, databaseName, tableName, tableDto)
        def retrievedTable = api.getTable(catalogName, databaseName, tableName, true, true, false)

        then:
        noExceptionThrown()
        retrievedTable != null
        retrievedTable.metadata != null
        retrievedTable.metadata.containsKey('table_type')
        retrievedTable.metadata.get('table_type') == 'ICEBERG'
        retrievedTable.metadata.containsKey('metadata_location')
        retrievedTable.metadata.get('metadata_location') == metadataLocation

        // Verify that v1 format tables created with Iceberg >= 0.14.1 DO support branches/tags
        retrievedTable.metadata.containsKey('iceberg.has.non.main.branches')
        retrievedTable.metadata.containsKey('iceberg.has.tags')

        // v1 format with refs should detect branches/tags correctly
        // This table has 2 branches (main, dev-branch) + 1 tag (v3.0.0)
        retrievedTable.metadata.get('iceberg.has.non.main.branches') == 'true'
        retrievedTable.metadata.get('iceberg.has.tags') == 'true'

        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)

        where:
        catalogName << ['polaris-metastore']
    }

    @Unroll
    def "Test database with empty metadata"() {
        given:
        def databaseName = 'iceberg_database'

        when:
        api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        def retrievedDatabase = api.getDatabase(catalogName, databaseName, true, true)

        then:
        noExceptionThrown()
        retrievedDatabase.getMetadata().isEmpty()

        cleanup:
        api.deleteDatabase(catalogName, databaseName)

        where:
        catalogName << ['polaris-metastore']
    }

    @Unroll
    def "Test database with existing metadata"() {
        given:
        def databaseName = 'iceberg_database'

        when:
        def databaseCreateRequestDto = DatabaseCreateRequestDto.builder()
        .metadata(ImmutableMap.of("key", "value"))
        .build()
        api.createDatabase(catalogName, databaseName, databaseCreateRequestDto)
        def retrievedDatabase = api.getDatabase(catalogName, databaseName, true, true)

        then:
        noExceptionThrown()
        retrievedDatabase.getMetadata().containsKey("key")
        retrievedDatabase.getMetadata().containsValue("value")

        cleanup:
        api.deleteDatabase(catalogName, databaseName)

        where:
        catalogName << ['polaris-metastore']
    }

    @Unroll
    def "Test database with updated metadata"() {
        given:
        def databaseName = 'iceberg_database'

        when:
        def databaseCreateRequestDto = DatabaseCreateRequestDto.builder()
            .metadata(ImmutableMap.of("key1", "value1"))
            .build()
        api.createDatabase(catalogName, databaseName, databaseCreateRequestDto)
        def databaseUpdatedRequestDto = DatabaseCreateRequestDto.builder()
            .metadata(ImmutableMap.of("key2", "value2"))
            .build()
        api.updateDatabase(catalogName, databaseName, databaseUpdatedRequestDto)
        def retrievedDatabase = api.getDatabase(catalogName, databaseName, true, true)

        then:
        noExceptionThrown()
        retrievedDatabase.getMetadata().containsKey("key1")
        retrievedDatabase.getMetadata().containsValue("value1")
        retrievedDatabase.getMetadata().containsKey("key2")
        retrievedDatabase.getMetadata().containsValue("value2")

        cleanup:
        api.deleteDatabase(catalogName, databaseName)

        where:
        catalogName << ['polaris-metastore']
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
        'polaris-metastore'             | 'smoke_db1'
        'polaris-metastore'             | 'fsmoke_db1'
        'polaris-metastore'             | 'shard'
        'polaris-metastore'             | 'hsmoke_db1'
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
        'polaris-metastore'       | 'smoke_db2'  | 'part'
        'polaris-metastore'  | 'fsmoke_db2' | 'part'
        'polaris-metastore'  | 'shard'      | 'part'
        'polaris-metastore'                | 'hsmoke_db2' | 'part'
        's3-mysql-db'                   | 'smoke_db2'  | 'part'
        's3-mysql-db'                   | 'smoke_db2'  | 'PART'
    }

    @Unroll
    def "Test rename table for #catalogName/#databaseName/#tableName"() {
        given:
        createTable(catalogName, databaseName, tableName, external)
        api.renameTable(catalogName, databaseName, tableName, newTableName)
        def table = api.getTable(catalogName, databaseName, newTableName, true, true, false)
        expect:
        table != null && table.name.tableName == newTableName
        table.getMetadata() != null
        // Polaris/Iceberg tables have table_type=ICEBERG, non-Polaris tables have EXTERNAL=TRUE
        if (catalogName.startsWith('polaris')) {
            assert table.getMetadata().get('table_type')?.equalsIgnoreCase('ICEBERG')
        } else {
            assert table.getMetadata().get('EXTERNAL')?.equalsIgnoreCase('TRUE')
        }
        table.getDefinitionMetadata() != null
        cleanup:
        api.deleteTable(catalogName, databaseName, newTableName)
        where:
        catalogName                     | databaseName | tableName            | external | newTableName
        'polaris-metastore'             | 'smoke_db3'  | 'test_create_table'  | null     | 'test_create_table1'
        'polaris-metastore'             | 'fsmoke_db3' | 'test_create_table'  | null     | 'test_create_table1'
        'polaris-metastore'             | 'shard'      | 'test_create_table'  | null     | 'test_create_table1'
        'polaris-metastore'             | 'shard'      | 'test_create_tablet' | 'TRUE'   | 'test_create_tablet1'
        'polaris-metastore'             | 'shard'      | 'test_create_tablef' | 'FALSE'  | 'test_create_tablef1'
        'polaris-metastore'             | 'hsmoke_db3' | 'test_create_table'  | null     | 'test_create_table1'
    }

    @Unroll
    def "Test drop and rename table"() {
        given:
        def tableName = 'test_create_table_rename'
        def tableName1 = 'test_create_table_rename1'
        def tableNameTagNoDelete = 'test_create_table_tag_no_delete'
        def tableNameTagNoRename = 'test_create_table_tag_no_rename'
        def tableNameTagNoRenamed = 'test_create_table_tag_no_renamed'
        createTable(catalogName, databaseName, tableName, 'TRUE')
        createTable(catalogName, databaseName, tableName1, 'TRUE')
        createTable(catalogName, databaseName, tableNameTagNoDelete, 'TRUE')
        createTable(catalogName, databaseName, tableNameTagNoRename, 'TRUE')
        api.deleteTable(catalogName, databaseName, tableName)
        when:
        api.renameTable(catalogName, databaseName, tableName1, tableName)
        then:
        noExceptionThrown()
        when:
        createTable(catalogName, databaseName, tableName1, 'TRUE')
        api.deleteTable(catalogName, databaseName, tableName)
        api.renameTable(catalogName, databaseName, tableName1, tableName)
        then:
        noExceptionThrown()
        when:
        api.renameTable(catalogName, databaseName, tableName, tableName1)
        then:
        noExceptionThrown()
        when:
        api.renameTable(catalogName, databaseName, tableName1, tableName)
        then:
        noExceptionThrown()
        def table = api.getTable(catalogName, databaseName, tableName, true, true, false)
        table.getMetadata() != null
        // Polaris/Iceberg tables have table_type=ICEBERG, non-Polaris tables have EXTERNAL=TRUE
        if (catalogName.startsWith('polaris')) {
            table.getMetadata().get('table_type')?.equalsIgnoreCase('ICEBERG')
        } else {
            table.getMetadata().get('EXTERNAL')?.equalsIgnoreCase('TRUE')
        }
        table.getDefinitionMetadata() != null
        when:
        tagApi.setTableTags(catalogName, databaseName, tableNameTagNoDelete, ['do_not_drop'] as Set)
        api.deleteTable(catalogName, databaseName, tableNameTagNoDelete)
        then:
        thrown(MetacatBadRequestException)
        when:
        tagApi.setTableTags(catalogName, databaseName, tableNameTagNoDelete, ['do_not_drop', 'iceberg_migration_do_not_modify'] as Set)
        api.deleteTable(catalogName, databaseName, tableNameTagNoDelete)
        then:
        thrown(RetryableException)
        when:
        tagApi.removeTableTags(catalogName, databaseName, tableNameTagNoDelete, true, [] as Set)
        api.deleteTable(catalogName, databaseName, tableNameTagNoDelete)
        then:
        noExceptionThrown()
        when:
        tagApi.setTableTags(catalogName, databaseName, tableNameTagNoRename, ['do_not_drop','do_not_rename'] as Set)
        api.renameTable(catalogName, databaseName, tableNameTagNoRename, tableNameTagNoRenamed)
        then:
        thrown(MetacatBadRequestException)
        when:
        tagApi.setTableTags(catalogName, databaseName, tableNameTagNoRename, ['do_not_drop','iceberg_migration_do_not_modify'] as Set)
        api.renameTable(catalogName, databaseName, tableNameTagNoRename, tableNameTagNoRenamed)
        then:
        thrown(RetryableException)
        when:
        tagApi.removeTableTags(catalogName, databaseName, tableNameTagNoRename, false, ['iceberg_migration_do_not_modify'] as Set)
        api.renameTable(catalogName, databaseName, tableNameTagNoRename, tableNameTagNoRenamed)
        tagApi.removeTableTags(catalogName, databaseName, tableNameTagNoRenamed, true, [] as Set)
        then:
        noExceptionThrown()
        cleanup:
        api.deleteTable(catalogName, databaseName, tableName1)
        api.deleteTable(catalogName, databaseName, tableNameTagNoRenamed)
        where:
        catalogName                    | databaseName
        'polaris-metastore' | 'hsmoke_db3'
        'polaris-metastore' | 'fsmoke_ddb1'
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
        // MView API is deprecated for Polaris/Iceberg - only test with non-Polaris catalogs
        catalogName                     | databaseName | tableName           | viewName    | error                          | repeat
        's3-mysql-db'                   | 'smoke_db4'  | 'part'              | 'part_view' | null                           | false
        's3-mysql-db'                   | 'smoke_db4'  | 'metacat_all_types' | 'part_view' | null                           | false
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
        // MView API is deprecated for Polaris/Iceberg - only test with non-Polaris catalogs
        catalogName                     | databaseName   | tableName           | viewName    | error                          | repeat
        's3-mysql-db'                   | 'smoke_db4'    | 'part'              | 'part_view' | null                           | false
        's3-mysql-db'                   | 'smoke_db4'    | 'metacat_all_types' | 'part_view' | null                           | false
        'xyz'                           | 'smoke_db4'    | 'z'                 | 'part_view' | MetacatNotFoundException.class | false
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
        api.getTable('invalid', 'invalid', 'invalid', true, false, false)
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
        noExceptionThrown()
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
        'polaris-metastore'       | 'smoke_db6'  | 'part'    | ['test'] as Set<String>           | true
        'polaris-metastore'       | 'smoke_db6'  | 'part'    | ['test', 'unused'] as Set<String> | false
        'polaris-metastore'  | 'fsmoke_db6' | 'part'    | ['test'] as Set<String>           | true
        'polaris-metastore'  | 'fsmoke_db6' | 'part'    | ['test', 'unused'] as Set<String> | false
        'polaris-metastore'  | 'shard'      | 'part'    | ['test'] as Set<String>           | true
        'polaris-metastore'  | 'shard'      | 'part'    | ['test', 'unused'] as Set<String> | false
        'polaris-metastore'                | 'hsmoke_db6' | 'part'    | ['test'] as Set<String>           | true
        'polaris-metastore'                | 'hsmoke_db6' | 'part'    | ['test', 'unused'] as Set<String> | false
        's3-mysql-db'                   | 'smoke_db6'  | 'part'    | ['test'] as Set<String>           | true
        's3-mysql-db'                   | 'smoke_db6'  | 'part'    | ['test', 'unused'] as Set<String> | false
    }

    @Unroll
    def "Test tags for catalogName, databaseName and tableName"() {
        when:
        createTable(catalogName, databaseName, tableName)
        def catalog = QualifiedName.ofCatalog(catalogName)
        tagApi.setTags(TagCreateRequestDto.builder().name(catalog).tags(tags).build())
        if (repeat) {
            tagApi.setTags(TagCreateRequestDto.builder().name(catalog).tags(tags).build())
        }

        def database = QualifiedName.ofDatabase(catalogName, databaseName)
        tagApi.setTags(TagCreateRequestDto.builder().name(database).tags(tags).build())

        def table = QualifiedName.ofTable(catalogName, databaseName, tableName)
        tagApi.setTags(TagCreateRequestDto.builder().name(table).tags(tags).build())

        def catalogDto = api.getCatalog(catalogName)
        def databaseDto = api.getDatabase(catalogName, databaseName, true, false)
        def tableDto = api.getTable(catalogName, databaseName, tableName, true, true, false)

        def ret = tagApi.search('test_tag', null, null, null)
        def ret2 = tagApi.list(['test_tag'] as Set<String>, null, null, null, null, null)
        def ret3 = tagApi.list(['never_tag'] as Set<String>, tags as Set<String>, null, null, null, null)
        def ret4 = tagApi.list(['test_tag'] as Set<String>, null, null, null, null, QualifiedName.Type.DATABASE)
        def ret5 = tagApi.list(['test_tag'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE)
        def ret6 = tagApi.list(['test_tag'] as Set<String>, null, null, null, null, QualifiedName.Type.CATALOG)

        def ret7 = tagApi.list(['test_tag'] as Set<String>, null, null, databaseName, null, QualifiedName.Type.CATALOG)
        def ret8 = tagApi.list(['test_tag'] as Set<String>, null, null, databaseName, null, QualifiedName.Type.DATABASE)

        tagApi.removeTags(TagRemoveRequestDto.builder().name(catalog).tags([]).deleteAll(true).build())
        tagApi.removeTags(TagRemoveRequestDto.builder().name(database).tags([]).deleteAll(true).build())
        tagApi.removeTags(TagRemoveRequestDto.builder().name(table).tags([]).deleteAll(true).build())

        def ret_new = tagApi.search('test_tag', null, null, null)
        def ret2_new = tagApi.list(['test_tag'] as Set<String>, null, null, null, null, null)

        then:
        metacatJson.convertValue(catalogDto.getDefinitionMetadata().get('tags'), Set.class) == tags as Set<String>
        metacatJson.convertValue(databaseDto.getDefinitionMetadata().get('tags'), Set.class) == tags as Set<String>
        metacatJson.convertValue(tableDto.getDefinitionMetadata().get('tags'), Set.class) == tags as Set<String>

        assert ret.size() == 3
        assert ret2.size() == 3
        assert ret4.size() == 1 //only query database
        assert ret5.size() == 1 //only query table
        assert ret6.size() == 1 //only query catalog
        assert ret3.size() == 0
        assert ret7.size() == 0 //limit to database
        assert ret8.size() == 1 //limit to database

        assert ret_new.size() == 0
        assert ret2_new.size() == 0

        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)

        where:
        catalogName                     | databaseName | tableName | tags                              | repeat
        'polaris-metastore'       | 'smoke_db6'  | 'part'    | ['test_tag']    as List<String>        | true
        'polaris-metastore'       | 'smoke_db6'  | 'part'    | ['test_tag', 'unused'] as List<String> | false
        'polaris-metastore'  | 'fsmoke_db6' | 'part'    | ['test_tag'] as List<String>           | true
        'polaris-metastore'  | 'fsmoke_db6' | 'part'    | ['test_tag', 'unused'] as List<String> | false
        'polaris-metastore'  | 'shard'      | 'part'    | ['test_tag'] as List<String>           | true
        'polaris-metastore'  | 'shard'      | 'part'    | ['test_tag', 'unused'] as List<String> | false
        'polaris-metastore'                | 'hsmoke_db6' | 'part'    | ['test_tag'] as List<String>           | true
        'polaris-metastore'                | 'hsmoke_db6' | 'part'    | ['test_tag', 'unused'] as List<String> | false
        's3-mysql-db'                   | 'smoke_db6'  | 'part'    | ['test_tag'] as List<String>           | true
        's3-mysql-db'                   | 'smoke_db6'  | 'part'    | ['test_tag', 'unused'] as List<String> | false
    }

    @Unroll
    def "Test tags for #catalogName/#databaseName/#tableName/#viewName"() {
        when:
        try {
            api.createDatabase(catalogName, 'franklinviews', new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }
        createTable(catalogName, databaseName, tableName)
        api.createMView(catalogName, databaseName, tableName, viewName, true, null)

        def view = QualifiedName.ofView(catalogName, databaseName, tableName, viewName)
        tagApi.setTags(TagCreateRequestDto.builder().name(view).tags(tags).build())

        def ret = tagApi.search('test_tag', null, null, null)
        def ret2 = tagApi.list(['test_tag'] as Set<String>, null, null, null, null, null)
        def ret3 = tagApi.list(['never_tag'] as Set<String>, tags as Set<String>, null, null, null, null)

        tagApi.removeTags(TagRemoveRequestDto.builder().name(view).tags([]).deleteAll(true).build())

        def ret_new = tagApi.search('test_tag', null, null, null)
        def ret2_new = tagApi.list(['test_tag'] as Set<String>, null, null, null, null, null)

        then:
        assert ret.size() == 1
        assert ret2.size() == 1
        assert ret3.size() == 0

        assert ret_new.size() == 0
        assert ret2_new.size() == 0

        cleanup:
            api.deleteMView(catalogName, databaseName, tableName, viewName)
        where:
        // MView API is deprecated for Polaris/Iceberg - only test with non-Polaris catalogs
        catalogName                     | databaseName | tableName           | viewName    |tags
        's3-mysql-db'                   | 'smoke_db4'  | 'part'              | 'part_view' | ['test_tag']    as List<String>
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
        'polaris-metastore/smoke_db/dm'       | false
        'polaris-metastore/smoke_db/dm'       | true
    }

    def "List definition metadata valid and invalid sortBy" () {
        given:
        def qName = "polaris-metastore/smoke_db/dm"
        when:
        metadataApi.getDefinitionMetadataList("zz_invalid", null, null, null, null, null, qName, null)
        then:
        thrown(MetacatBadRequestException)
        when:
        metadataApi.getDefinitionMetadataList("created_by", null, null, null, null, null, qName, null)
        then:
        noExceptionThrown()
        when:
        metadataApi.getDefinitionMetadataList("created_by", SortOrder.ASC, null, null, null, null, qName, null)
        then:
        noExceptionThrown()
    }

    @Unroll
    @Ignore
    def "Delete definition metadata for non-existant #name"() {
        when:
        def metacatSource = Sql.newInstance('jdbc:mysql://metacat-db:3306/metacat?useUnicode=true&characterEncoding=latin1&autoReconnect=true&sessionVariables=@@innodb_lock_wait_timeout=120&rewriteBatchedStatements=true', 'metacat_user','metacat_user_password', 'com.mysql.jdbc.Driver')
        metacatSource.execute("insert into definition_metadata(version,created_by,data,date_created,last_updated,last_updated_by,name) values (0,'test', '{}',now(), now(), 'test'," + name+ ")")
        metadataApi.deleteDefinitionMetadata(name, false)
        def list = metadataApi.getDefinitionMetadataList(null, null, null, null, null, null, name, null)
        then:
        list.isEmpty()
        where:
        name << ['polaris-metastore/invalid/dm',
                 'polaris-metastore/invalid/dm/vm',
                 'polaris-metastore/invalid/dm/vm=1']
    }

    def "Test Set tags for all scenarios"() {
        setup:
        // The setup already contains the following 7 tags
        // [data-category:non-customer, test, test_tag, unused, do_not_drop, iceberg_migration_do_not_modify, do_not_rename]
        def catalogName = "polaris-metastore"
        def databaseName = "default"
        def catalog = QualifiedName.ofCatalog(catalogName)
        def database = QualifiedName.ofDatabase(catalogName, databaseName)

        def table1Name = "table1"
        def table1Tags = ['table1', 'mock'] as Set

        def table2Name = "table2"
        def table2Tags = ['table2', 'mock'] as Set

        def table3Name = "table3"
        def table3Tags = ['table3', 'bdow'] as Set
        when:
        // add 2 new tags on table1
        createTable(catalogName, databaseName, table1Name)
        tagApi.setTableTags(catalogName, databaseName, table1Name, table1Tags)
        then:
        tagApi.getTags().size() == 9
        tagApi.list(['table1', 'mock'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 1
        tagApi.search('mock', null, null, null).size() == 1
        tagApi.search('table1', null, null, null).size() == 1
        metacatJson.convertValue(api.getTable(catalogName, databaseName, table1Name, false, true, true).getDefinitionMetadata().get('tags'), Set.class) == table1Tags
        when:
        // add an existing tag mock but a new tag on table 2
        createTable(catalogName, databaseName, table2Name)
        tagApi.setTableTags(catalogName, databaseName, table2Name, table2Tags)
        then:
        tagApi.getTags().size() == 10
        tagApi.list(['table2'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 1
        tagApi.list(['mock'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 2
        tagApi.list(['table1', 'mock'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 2
        tagApi.search('mock', null, null, null).size() == 2
        tagApi.search('table1', null, null, null).size() == 1
        tagApi.search('table2', null, null, null).size() == 1
        metacatJson.convertValue(api.getTable(catalogName, databaseName, table2Name, false, true, true).getDefinitionMetadata().get('tags'), Set.class) == table2Tags
        when:
        // delete table1, for now even if tag table1 is only applied to table1, we will not remove the tag from our db
        api.deleteTable(catalogName, databaseName, "table1")
        then:
        tagApi.getTags().size() == 10
        tagApi.list(['table1'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 1
        tagApi.list(['table2', 'mock'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 2
        tagApi.list(['mock'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 2
        tagApi.search('mock', null, null, null).size() == 2
        tagApi.search('table1', null, null, null).size() == 1
        tagApi.search('table2', null, null, null).size() == 1
        when:
        // add tags on catalog database and table level through setTags api
        // create 1 new catalogTag on catalog
        // create 1 new databaseTag on database
        // create 2 new tableTags on table3
        tagApi.setTags(TagCreateRequestDto.builder().name(catalog).tags(['mock', 'catalogTag'] as List<String>).build())
        tagApi.setTags(TagCreateRequestDto.builder().name(database).tags(['mock', 'databaseTag'] as List<String>).build())
        createTable(catalogName, databaseName, "table3")
        def table3 = QualifiedName.ofTable(catalogName, databaseName, table3Name)
        tagApi.setTags(TagCreateRequestDto.builder().name(table3).tags(table3Tags as List<String>).build())
        then:
        tagApi.getTags().size() == 14
        tagApi.list(['table2', 'mock'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 2
        tagApi.list(['table3', 'bdow'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 1
        tagApi.list(['mock'] as Set<String>, null, null, null, null, QualifiedName.Type.TABLE).size() == 2
        tagApi.list(['mock'] as Set<String>, null, null, null, null, QualifiedName.Type.DATABASE).size() == 1
        tagApi.list(['mock'] as Set<String>, null, null, null, null, QualifiedName.Type.CATALOG).size() == 1
        tagApi.list(['mock'] as Set<String>, null, null, null, null, null).size() == 4
        tagApi.search('mock', null, null, null).size() == 4
        tagApi.search('table3', null, null, null).size() == 1
        tagApi.search('bdow', null, null, null).size() == 1
        tagApi.search('table2', null, null, null).size() == 1
        metacatJson.convertValue(api.getTable(catalogName, databaseName, table3Name, false, true, true).getDefinitionMetadata().get('tags'), Set.class) == table3Tags
        cleanup:
        api.deleteTable(catalogName, databaseName, "table2")
        api.deleteTable(catalogName, databaseName, "table3")
    }

    def 'testCloneTableE2E'() {
        given:
        def catalogName = 'polaris-metastore'
        def databaseName = 'iceberg_db'

        // First parent child connected component
        def parent1 = "parent1"
        def parent1UUID = "p1_uuid"
        def renameParent1 = "rename_parent1"
        def parent1Uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, parent1) : null
        def renameParent1Uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, parent1) : null
        def parent1FullName = catalogName + "/" + databaseName + "/" + parent1
        def renameParent1FullName = catalogName + "/" + databaseName + "/" + renameParent1

        def child11 = "child11"
        def child11UUID = "c11_uuid"
        def renameChild11 = "rename_child11"
        def child11Uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, child11) : null
        def renameChild11Uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, renameChild11) : null
        def child11FullName = catalogName + "/" + databaseName + "/" + child11

        def child12 = "child12"
        def child12UUID = "c12_uuid"
        def child12Uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, child12) : null

        def grandChild121 = "grandchild121"
        def grandChild121UUID= "gc121_uuid"

        def grandParent1 = "grandparent1"
        def grandParent1FullName =  catalogName + "/" + databaseName + "/" + grandParent1
        def grandParent1UUID = "grandparent2_uuid"
        def grantParent1Uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, parent1) : null


        // Second parent child connected component
        def parent2 = "parent2"
        def parent2UUID = "p2_uuid"
        def parent2Uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, parent2) : null
        def parent2FullName = catalogName + "/" + databaseName + "/" + parent2
        def child21 = "child21"
        def child21UUID = "c21_uuid"
        def child21Uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, child21) : null

        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }

        /*
        Step 1: Create one Parent (parent1) and one Child (child11)
         */
        when:
        // Create Parent1
        def parent1TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, parent1, 'amajumdar', parent1Uri)
        api.createTable(catalogName, databaseName, parent1, parent1TableDto)

        // Create child11 Table with parent = parent1
        def child11TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, child11, 'amajumdar', child11Uri)
        initializeParentChildRelDefinitionMetadata(child11TableDto, parent1FullName, parent1UUID, child11UUID)
        child11TableDto.definitionMetadata.put("random_key", "random_value")
        api.createTable(catalogName, databaseName, child11, child11TableDto)

        def parent1Table = api.getTable(catalogName, databaseName, parent1, true, true, false)
        def child11Table = api.getTable(catalogName, databaseName, child11, true, true, false)
        then:
        // Test Parent 1 parentChildInfo
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent1) == [new ChildInfoDto("polaris-metastore/iceberg_db/child11", "CLONE", "c11_uuid")] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent1).isEmpty()

        // Test Child11 parentChildInfo
        assert !child11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        assert child11Table.definitionMetadata.get("random_key").asText() == "random_value"
        JSONAssert.assertEquals(child11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent1","relationType":"CLONE", "uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getParents(catalogName, databaseName, child11) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent1", "CLONE", "p1_uuid")] as Set
        assert parentChildRelV1.getChildren(catalogName, databaseName, child11).isEmpty()

        /*
        Step 2: create another table with the same parent1 name but should fail because the table already exists
        Test this should not impact the previous record
         */
        when:
        api.createTable(catalogName, databaseName, parent1, parent1TableDto)
        then:
        def e = thrown(Exception)
        assert e.message.contains("already exists")

        when:
        parent1Table = api.getTable(catalogName, databaseName, parent1, true, true, false)
        child11Table = api.getTable(catalogName, databaseName, child11, true, true, false)
        then:
        // Test Parent 1 parentChildInfo
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent1) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child11", "CLONE", "c11_uuid"),
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent1).isEmpty()
        // Test Child11 parentChildInfo
        assert !child11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        assert child11Table.definitionMetadata.get("random_key").asText() == "random_value"
        JSONAssert.assertEquals(child11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent1","relationType":"CLONE", "uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child11) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent1", "CLONE", "p1_uuid")] as Set

        /*
        Step 3: create another table with the same child1 name but different uuid under the same parent should fail
         */
        when:
        child11TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, child11, 'amajumdar', child11Uri)
        initializeParentChildRelDefinitionMetadata(child11TableDto, parent1FullName, parent1UUID, "random_uuid")
        api.createTable(catalogName, databaseName, child11, child11TableDto)
        then:
        e = thrown(Exception)
        assert e.message.contains("Cannot have a child table having more than one parent")

        /*
        Step 4: create another table with the same child1 name but different uuid under a different parent that
        does not exist
        */
        when:
        child11TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, child11, 'amajumdar', child11Uri)
        initializeParentChildRelDefinitionMetadata(child11TableDto, parent2FullName, parent2UUID, "random_uuid")
        api.createTable(catalogName, databaseName, child11, child11TableDto)
        then:
        e = thrown(Exception)
        assert e.message.contains("does not exist")

        /*
        Step 5: create another table with the same child1 name but different uuid under a different parent that
        does exists
        */
        when:
        def randomParentName = "randomParent"
        def randomParentUUID = "randomParent_uuid"
        def randomParentFullName = catalogName + "/" + databaseName + "/" + randomParentName
        def randomParentUri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, randomParentName) : null
        def randomParentTableDto = PigDataDtoProvider.getTable(catalogName, databaseName, randomParentName, 'amajumdar', randomParentUri)
        api.createTable(catalogName, databaseName, randomParentName, randomParentTableDto)
        child11TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, child11, 'amajumdar', child11Uri)
        initializeParentChildRelDefinitionMetadata(child11TableDto, randomParentFullName, randomParentUUID, "random_uuid")
        api.createTable(catalogName, databaseName, child11, child11TableDto)
        then:
        e = thrown(Exception)
        assert e.message.contains("Cannot have a child table having more than one parent")

        /*
        Step 6: create another table with the same name different uuid without specifying the parent child relation
        but should fail because the table already exists
        This test the failure during creation should not impact the previous record
         */
        when:
        child11TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, child11, 'amajumdar', child11Uri)
        api.createTable(catalogName, databaseName, child11, child11TableDto)
        then:
        e = thrown(Exception)
        assert e.message.contains("already exists")

        when:
        parent1Table = api.getTable(catalogName, databaseName, parent1, true, true, false)
        child11Table = api.getTable(catalogName, databaseName, child11, true, true, false)
        then:
        // Test Parent 1 parentChildInfo
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent1) == [new ChildInfoDto("polaris-metastore/iceberg_db/child11", "CLONE", "c11_uuid")] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent1).isEmpty()

        // Test Child11 parentChildInfo
        assert !child11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        assert child11Table.definitionMetadata.get("random_key").asText() == "random_value"
        JSONAssert.assertEquals(child11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent1","relationType":"CLONE", "uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child11) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent1", "CLONE", "p1_uuid")] as Set


        /*
        Step 7: Create a second child (child12) pointing to parent = parent1
         */
        when:
        // Create Child2 Table
        def child12TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, child12, 'amajumdar', child12Uri)
        initializeParentChildRelDefinitionMetadata(child12TableDto, parent1FullName, parent1UUID, child12UUID)
        api.createTable(catalogName, databaseName, child12, child12TableDto)
        parent1Table = api.getTable(catalogName, databaseName, parent1, true, true, false)
        def child12Table = api.getTable(catalogName, databaseName, child12, true, true, false)

        then:
        // Test Parent 1 parentChildInfo
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent1) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child11", "CLONE", "c11_uuid"),
            new ChildInfoDto("polaris-metastore/iceberg_db/child12", "CLONE", "c12_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent1).isEmpty()

        // Test Child12 parentChildInfo
        assert !child12Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child12Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child12).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child12) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent1", "CLONE", "p1_uuid")] as Set

        /*
        Step 8: create a parent table on top of another parent table should fail
         */
        when:
        def grandParent1TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, grandParent1, 'amajumdar', grantParent1Uri)
        api.createTable(catalogName, databaseName, grandParent1, grandParent1TableDto)

        def parent1TableDtoCopy = PigDataDtoProvider.getTable(catalogName, databaseName, parent1, 'amajumdar', parent1Uri)
        initializeParentChildRelDefinitionMetadata(parent1TableDtoCopy, grandParent1FullName, grandParent1UUID, parent1UUID)
        api.createTable(catalogName, databaseName, parent1, parent1TableDtoCopy)
        then:
        e = thrown(Exception)
        assert e.message.contains("Cannot create a parent table on top of another parent")

        /*
        Step 9: Create one grandChild As a Parent of A child table should fail
         */
        when:
        def grandchild121TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, grandChild121, 'amajumdar', null)
        initializeParentChildRelDefinitionMetadata(grandchild121TableDto, child11FullName, child11UUID, grandChild121UUID)
        api.createTable(catalogName, databaseName, grandChild121, grandchild121TableDto)
        assert parentChildRelV1.getChildren(catalogName, databaseName, grandChild121).isEmpty()
        then:
        e = thrown(Exception)
        assert e.message.contains("Cannot create a child table as parent")

        /*
        Step 10: Create another parent child that is disconnected with the above
         */
        when:
        // Create Parent2
        def parent2TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, parent2, 'amajumdar', parent2Uri)
        api.createTable(catalogName, databaseName, parent2, parent2TableDto)

        // Create child21 Table with parent = parent2
        def child21TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, child21, 'amajumdar', child21Uri)
        initializeParentChildRelDefinitionMetadata(child21TableDto, parent2FullName, parent2UUID, child21UUID)
        api.createTable(catalogName, databaseName, child21, child21TableDto)
        def parent2Table = api.getTable(catalogName, databaseName, parent2, true, true, false)
        def child21Table = api.getTable(catalogName, databaseName, child21, true, true, false)

        then:
        // Test Parent 2 parentChildInfo
        assert parent2Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent2) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child21", "CLONE", "c21_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent2).isEmpty()

        // Test Child21 parentChildInfo
        JSONAssert.assertEquals(child21Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent2","relationType":"CLONE","uuid":"p2_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child21).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child21) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent2", "CLONE", "p2_uuid")] as Set

        /*
        Step 11: Create a table newParent1 without any parent child rel info
        and attempt to rename parent1 to newParent1 should fail
        Test the parentChildRelationship record remain the same after the revert
         */
        when:
        def newParent1TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, renameParent1, 'amajumdar', renameParent1Uri)
        api.createTable(catalogName, databaseName, renameParent1, newParent1TableDto)
        api.renameTable(catalogName, databaseName, parent1, renameParent1)
        then:
        e = thrown(Exception)
        assert e.message.contains("already exists")

        when:
        parent1Table = api.getTable(catalogName, databaseName, parent1, true, true, false)
        child11Table = api.getTable(catalogName, databaseName, child11, true, true, false)
        child12Table = api.getTable(catalogName, databaseName, child12, true, true, false)

        then:
        // Test Parent 1 parentChildInfo
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent1) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child11", "CLONE", "c11_uuid"),
            new ChildInfoDto("polaris-metastore/iceberg_db/child12", "CLONE", "c12_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent1).isEmpty()
        // Test Child11 parentChildInfo
        assert !child11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        assert child11Table.definitionMetadata.get("random_key").asText() == "random_value"
        JSONAssert.assertEquals(child11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent1","relationType":"CLONE", "uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child11) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent1", "CLONE", "p1_uuid")] as Set
        // Test Child12 parentChildInfo
        assert !child12Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child12Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child12).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child12) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent1", "CLONE", "p1_uuid")] as Set

        /*
         Step 12: Attempt to rename parent1 to parent2 which has parent child relationship and should fail
          Test the parentChildRelationship record remain the same after the revert
         */
        when:
        api.renameTable(catalogName, databaseName, parent1, parent2)
        then:
        e = thrown(Exception)
        assert e.message.contains("is already a parent table")

        when:
        parent1Table = api.getTable(catalogName, databaseName, parent1, true, true, false)
        child11Table = api.getTable(catalogName, databaseName, child11, true, true, false)
        child12Table = api.getTable(catalogName, databaseName, child12, true, true, false)
        parent2Table = api.getTable(catalogName, databaseName, parent2, true, true, false)
        child21Table = api.getTable(catalogName, databaseName, child21, true, true, false)
        then:
        // Test Parent 1 parentChildInfo
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent1) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child11", "CLONE", "c11_uuid"),
            new ChildInfoDto("polaris-metastore/iceberg_db/child12", "CLONE", "c12_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent1).isEmpty()
        // Test Child11 parentChildInfo
        assert !child11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        assert child11Table.definitionMetadata.get("random_key").asText() == "random_value"
        JSONAssert.assertEquals(child11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent1","relationType":"CLONE", "uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child11) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent1", "CLONE", "p1_uuid")] as Set
        // Test Child12 parentChildInfo
        assert !child12Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child12Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child12).isEmpty()
        // Test Parent 2 parentChildInfo
        assert parent2Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent2) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child21", "CLONE", "c21_uuid")
        ] as Set
        // Test Child21 parentChildInfo
        JSONAssert.assertEquals(child21Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent2","relationType":"CLONE","uuid":"p2_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child21).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child12) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent1", "CLONE", "p1_uuid")] as Set


        /*
        Step 13: First drop the newParent1 and then Rename parent1 to newParent1 should now succeed
         */
        when:
        api.deleteTable(catalogName, databaseName, renameParent1)
        api.renameTable(catalogName, databaseName, parent1, renameParent1)
        parent1Table = api.getTable(catalogName, databaseName, renameParent1, true, true, false)
        child11Table = api.getTable(catalogName, databaseName, child11, true, true, false)
        child12Table = api.getTable(catalogName, databaseName, child12, true, true, false)

        then:
        // Test Parent 1 parentChildInfo newName
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameParent1) ==
            [
                new ChildInfoDto("polaris-metastore/iceberg_db/child11", "CLONE", "c11_uuid"),
                new ChildInfoDto("polaris-metastore/iceberg_db/child12", "CLONE", "c12_uuid")
            ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, renameParent1).isEmpty()
        // Test Child11 parentChildInfo
        assert !child11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/rename_parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child11) == [new ParentInfoDto("polaris-metastore/iceberg_db/rename_parent1", "CLONE", "p1_uuid")] as Set

        // Test Child12 parentChildInfo
        assert !child12Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child12Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/rename_parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child12).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child12) == [new ParentInfoDto("polaris-metastore/iceberg_db/rename_parent1", "CLONE", "p1_uuid")] as Set

        //get the parent oldName should fail as it no longer exists
        when:
        api.getTable(catalogName, databaseName, parent1, true, true, false)
        then:
        e = thrown(Exception)
        assert e.message.contains("Unable to locate for")

        /*
        Step 14: Create a table renameChild11 without parent childInfo and then try to rename child11 to renameChild11, which should fail
        This test to make sure the revert works properly.
         */
        when:
        def newChild1TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, renameChild11, 'amajumdar', renameChild11Uri)
        api.createTable(catalogName, databaseName, renameChild11, newChild1TableDto)
        api.renameTable(catalogName, databaseName, child11, renameChild11)

        then:
        e = thrown(Exception)
        assert e.message.contains("already exists")

        when:
        parent1Table = api.getTable(catalogName, databaseName, renameParent1, true, true, false)
        child11Table = api.getTable(catalogName, databaseName, child11, true, true, false)
        child12Table = api.getTable(catalogName, databaseName, child12, true, true, false)
        then:
        // Test Parent 1 parentChildInfo newName
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameParent1) ==
            [
                new ChildInfoDto("polaris-metastore/iceberg_db/child11", "CLONE", "c11_uuid"),
                new ChildInfoDto("polaris-metastore/iceberg_db/child12", "CLONE", "c12_uuid")
            ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, renameParent1).isEmpty()
        // Test Child11 parentChildInfo
        assert !child11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/rename_parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child11) == [new ParentInfoDto("polaris-metastore/iceberg_db/rename_parent1", "CLONE", "p1_uuid")] as Set

        // Test Child12 parentChildInfo
        assert !child12Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child12Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/rename_parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child12).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child12) == [new ParentInfoDto("polaris-metastore/iceberg_db/rename_parent1", "CLONE", "p1_uuid")] as Set

        /*
        Step 15: Create a table renameChild11 with parent childInfo and then try to rename child11 to renameChild11, which should fail
        This test to make sure the revert works properly.
         */
        when:
        api.deleteTable(catalogName, databaseName, renameChild11)
        newChild1TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, renameChild11, 'amajumdar', renameChild11Uri)
        initializeParentChildRelDefinitionMetadata(newChild1TableDto, renameParent1FullName, parent1UUID, "random_uuid")
        api.createTable(catalogName, databaseName, renameChild11, newChild1TableDto)
        api.renameTable(catalogName, databaseName, child11, renameChild11)

        then:
        e = thrown(Exception)
        assert e.message.contains("is already a child table")

        when:
        def renameChild11Table = api.getTable(catalogName, databaseName, renameChild11, true, true, false)
        parent1Table = api.getTable(catalogName, databaseName, renameParent1, true, true, false)
        child11Table = api.getTable(catalogName, databaseName, child11, true, true, false)
        child12Table = api.getTable(catalogName, databaseName, child12, true, true, false)
        then:
        // Test Parent 1 parentChildInfo newName
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameParent1) ==
            [
                new ChildInfoDto("polaris-metastore/iceberg_db/child11", "CLONE", "c11_uuid"),
                new ChildInfoDto("polaris-metastore/iceberg_db/child12", "CLONE", "c12_uuid"),
                new ChildInfoDto("polaris-metastore/iceberg_db/rename_child11", "CLONE", "random_uuid")
            ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, renameParent1).isEmpty()
        // Test Child11 parentChildInfo
        assert !child11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/rename_parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child11) == [new ParentInfoDto("polaris-metastore/iceberg_db/rename_parent1", "CLONE", "p1_uuid")] as Set
        // Test Child12 parentChildInfo
        assert !child12Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child12Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/rename_parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child12).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child12) == [new ParentInfoDto("polaris-metastore/iceberg_db/rename_parent1", "CLONE", "p1_uuid")] as Set
        // Test renameChild11Table parentChildInfo
        assert !renameChild11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(renameChild11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/rename_parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameChild11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, renameChild11) == [new ParentInfoDto("polaris-metastore/iceberg_db/rename_parent1", "CLONE", "p1_uuid")] as Set


        /*
        Step 16: Drop previous renameChild11 and Rename child11 to renameChild11 should now succeed
         */
        when:
        api.deleteTable(catalogName, databaseName, renameChild11)
        api.renameTable(catalogName, databaseName, child11, renameChild11)
        parent1Table = api.getTable(catalogName, databaseName, renameParent1, true, true, false)
        child11Table = api.getTable(catalogName, databaseName, renameChild11, true, true, false)

        then:
        // Test parent1Table parentChildInfo with newName
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameParent1) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/rename_child11", "CLONE", "c11_uuid"),
            new ChildInfoDto("polaris-metastore/iceberg_db/child12", "CLONE", "c12_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, renameParent1).isEmpty()
        // Test Child11 parentChildInfo with newName
        assert !child11Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        assert child11Table.definitionMetadata.get("random_key").asText() == "random_value"
        JSONAssert.assertEquals(child11Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/rename_parent1","relationType":"CLONE","uuid":"p1_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameChild11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, renameChild11) == [new ParentInfoDto("polaris-metastore/iceberg_db/rename_parent1", "CLONE", "p1_uuid")] as Set

        //get the child oldName should fail as it no longer exists
        when:
        api.getTable(catalogName, databaseName, child11, true, true, false)
        then:
        e = thrown(Exception)
        assert e.message.contains("Unable to locate for")

        /*
        Step 17: Drop parent renameParent1
         */
        when:
        api.deleteTable(catalogName, databaseName, renameParent1)

        then:
        e = thrown(Exception)
        assert e.message.contains("because it still has")

        /*
        Step 18: Drop renameChild11 should succeed
         */
        when:
        api.deleteTable(catalogName, databaseName, renameChild11)
        parent1Table = api.getTable(catalogName, databaseName, renameParent1, true, true, false)

        then:
        // Test parent1 Table
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameParent1) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child12", "CLONE", "c12_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, renameParent1).isEmpty()

        /*
        Step 19: Create renameChild11 and should expect random_key should appear at it is reattached
        but parent childInfo should not as it is always coming from the parent child relationship service
        which currently does not have any record
         */
        when:
        child11TableDto = PigDataDtoProvider.getTable(catalogName, databaseName, renameChild11, 'amajumdar', child11Uri)
        api.createTable(catalogName, databaseName, renameChild11, child11TableDto)
        child11Table = api.getTable(catalogName, databaseName, renameChild11, true, true, false)
        parent1Table = api.getTable(catalogName, databaseName, renameParent1, true, true, false)
        then:
        assert !child11Table.definitionMetadata.has("parentChildRelationInfo")
        assert child11Table.definitionMetadata.get("random_key").asText() == "random_value"
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameChild11).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, renameChild11).isEmpty()
        // Test parent1 Table still only have child12
        assert parent1Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameParent1) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child12", "CLONE", "c12_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, renameParent1).isEmpty()

        /*
        Step 20: Drop child12 should succeed
         */
        when:
        api.deleteTable(catalogName, databaseName, child12)
        parent1Table = api.getTable(catalogName, databaseName, renameParent1, true, true, false)
        then:
        assert !parent1Table.definitionMetadata.has("parentChildRelationInfo")
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameParent1).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, renameParent1).isEmpty()

        /*
        Step 21: Drop renameParent1 should succeed as there is no more child under it
         */
        when:
        api.deleteTable(catalogName, databaseName, renameParent1)
        parent2Table = api.getTable(catalogName, databaseName, parent2, true, true, false)
        child21Table = api.getTable(catalogName, databaseName, child21, true, true, false)

        then:
        //Since renameParent1 table is dropped
        assert parentChildRelV1.getChildren(catalogName, databaseName, renameParent1).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, renameParent1).isEmpty()
        // Since all the operations above are on the first connected relationship, the second connected relationship
        // should remain the same
        // Test Parent 2 parentChildInfo
        assert parent2Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent2) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child21", "CLONE", "c21_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent2).isEmpty()

        // Test Child21 parentChildInfo
        assert !child21Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child21Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent2","relationType":"CLONE","uuid":"p2_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child21).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child21) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent2", "CLONE", "p2_uuid")] as Set

        /*
        Step 22: update parent2 with random parentChildRelationInfo to test immutability
         */
        when:
        def updateParent2Dto = parent2Table
        initializeParentChildRelDefinitionMetadata(updateParent2Dto, "RANDOM", "RANDOM", "RANDOM")
        api.updateTable(catalogName, databaseName, parent2, updateParent2Dto)

        parent2Table = api.getTable(catalogName, databaseName, parent2, true, true, false)
        child21Table = api.getTable(catalogName, databaseName, child21, true, true, false)
        then:
        assert parent2Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent2) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child21", "CLONE", "c21_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent2).isEmpty()
        // Test Child21 parentChildInfo
        assert !child21Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child21Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent2","relationType":"CLONE","uuid":"p2_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child21).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child21) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent2", "CLONE", "p2_uuid")] as Set

        /*
        Step 23: update child21 with random parentChildRelationInfo to test immutability
         */
        when:
        def updateChild21Dto = child21Table
        initializeParentChildRelDefinitionMetadata(updateParent2Dto, "RANDOM", "RANDOM", "RANDOM")
        api.updateTable(catalogName, databaseName, child21, updateChild21Dto)

        parent2Table = api.getTable(catalogName, databaseName, parent2, true, true, false)
        child21Table = api.getTable(catalogName, databaseName, child21, true, true, false)
        then:
        // Test Parent 2 parentChildInfo
        assert parent2Table.definitionMetadata.get("parentChildRelationInfo").get("isParent").booleanValue()
        assert parentChildRelV1.getChildren(catalogName, databaseName, parent2) == [
            new ChildInfoDto("polaris-metastore/iceberg_db/child21", "CLONE", "c21_uuid")
        ] as Set
        assert parentChildRelV1.getParents(catalogName, databaseName, parent2).isEmpty()

        // Test Child21 parentChildInfo
        assert !child21Table.definitionMetadata.get("parentChildRelationInfo").has("isParent")
        JSONAssert.assertEquals(child21Table.definitionMetadata.get("parentChildRelationInfo").toString(),
            '{"parentInfos":[{"name":"polaris-metastore/iceberg_db/parent2","relationType":"CLONE","uuid":"p2_uuid"}]}',
            false)
        assert parentChildRelV1.getChildren(catalogName, databaseName, child21).isEmpty()
        assert parentChildRelV1.getParents(catalogName, databaseName, child21) == [new ParentInfoDto("polaris-metastore/iceberg_db/parent2", "CLONE", "p2_uuid")] as Set
    }

    def "Test remove migrated_data_location before soft delete"() {
        given:
        def catalogName = 'polaris-metastore'
        def databaseName = 'smoke_db'
        def tableName = 'test_table_migrated_data_location'
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'gtret', uri)

        // Add multiple metadata fields to verify they persist
        tableDto.definitionMetadata.put('migrated_data_location', 's3://old/location')
        tableDto.definitionMetadata.put('preserved_field', 'should remain')
        tableDto.definitionMetadata.put('another_field', 'should also remain')

        try {
            api.createDatabase(catalogName, databaseName, new DatabaseCreateRequestDto())
        } catch (Exception ignored) {
        }

        when:
        def createdTable = api.createTable(catalogName, databaseName, tableName, tableDto)

        then:
        assert createdTable.definitionMetadata.has('migrated_data_location')
        assert createdTable.definitionMetadata.has('preserved_field')
        assert createdTable.definitionMetadata.has('another_field')

        // Soft delete the table
        when:
        api.deleteTable(catalogName, databaseName, tableName)

        // Recreate the table with the same name and verify migrated_data_location is gone
        def newTableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'gtret', uri)
        def recreatedTable = api.createTable(catalogName, databaseName, tableName, newTableDto)

        then:
        // Verify migrated_data_location is not present
        !recreatedTable.definitionMetadata.has('migrated_data_location')
        // Verify other fields are still present
        recreatedTable.definitionMetadata.has('preserved_field')
        recreatedTable.definitionMetadata.get('preserved_field').asText() == 'should remain'
        recreatedTable.definitionMetadata.has('another_field')
        recreatedTable.definitionMetadata.get('another_field').asText() == 'should also remain'

        // Verify the definition metadata doesn't contain migrated_data_location
        def definitionMetadatas = metadataApi.getDefinitionMetadataList(null, null, null, null, null, null, "$catalogName/$databaseName/$tableName", null)
        definitionMetadatas.each { definition ->
            assert !definition.getDefinitionMetadata().has("migrated_data_location")
        }

        // Verify no exceptions are thrown
        noExceptionThrown()

        cleanup:
        try {
            api.deleteTable(catalogName, databaseName, tableName)
        } catch (Exception ignored) {
        }
    }

    def "Test cross catalog db operations"() {
        given:
        def polaris_metastore = "polaris-metastore"
        def polaris_metastore_test = "polaris-metastore-test"
        def db_name_1 = "db1"
        def db_name_2 = "db2"

        def polaris_metastore_db1_qname = QualifiedName.ofDatabase(polaris_metastore, db_name_1)
        def polaris_metastore_test_db1_qname = QualifiedName.ofDatabase(polaris_metastore_test, db_name_1)
        def polaris_metastore_test_db2_qname = QualifiedName.ofDatabase(polaris_metastore_test, db_name_2)

        // Create Database
        try{
            api.createDatabase(polaris_metastore, db_name_1, new DatabaseCreateRequestDto())
            api.createDatabase(polaris_metastore_test, db_name_1, new DatabaseCreateRequestDto())
            api.createDatabase(polaris_metastore_test, db_name_2, new DatabaseCreateRequestDto())
        } catch (Exception e) {

        }

        // Get the database
        when:
        def polaris_metastore_db_1 = api.getDatabase(polaris_metastore, db_name_1, true, true)
        def polaris_metastore_test_db_1 = api.getDatabase(polaris_metastore_test, db_name_1, true, true)
        def polaris_metastore_test_db_2 = api.getDatabase(polaris_metastore_test, db_name_2, true, true)
        then:
        Assertions.assertEquals(polaris_metastore_db_1.name, polaris_metastore_db1_qname)
        Assertions.assertEquals(polaris_metastore_test_db_1.name, polaris_metastore_test_db1_qname)
        Assertions.assertEquals(polaris_metastore_test_db_2.name, polaris_metastore_test_db2_qname)

        // add update once it is not an no-op

        // list the databases
        Thread.sleep(5000)
        def polaris_metastore_dto = api.getCatalog(polaris_metastore)
        def polaris_metastore_test_dto = api.getCatalog(polaris_metastore_test)

        def expected_polaris_metastore_databases = ["db1"] as Set
        def expected_polaris_metastore_test_databases = ["db1", "db2"] as Set
        assert polaris_metastore_dto.getDatabases().toSet().containsAll(expected_polaris_metastore_databases)
        assert polaris_metastore_test_dto.getDatabases().toSet().containsAll(expected_polaris_metastore_test_databases)

        api.deleteDatabase(polaris_metastore, db_name_1)
        api.deleteDatabase(polaris_metastore_test, db_name_1)
        api.deleteDatabase(polaris_metastore_test, db_name_2)
    }

    def "Test cross catalog table operations"() {
        given:
        def polaris_metastore = "polaris-metastore"
        def polaris_metastore_test = "polaris-metastore-test"
        def db_name_1 = "cross_catalog_db1"

        def tbl_name_1 = "table_1"
        def new_tbl_name_1 = "new_table_1"
        def tbl_name_2 = "table_2"

        def metadata_location = 'file:/tmp/data/metadata/00000-0b60cc39-438f-413e-96c2-2694d7926529.metadata.json'
        def updated_metadata_location = 'file:/tmp/data/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json'
        def initial_metadata = [table_type: 'ICEBERG', metadata_location: metadata_location]
        def updated_metadata = [table_type: 'ICEBERG', metadata_location: updated_metadata_location, previous_metadata_location: metadata_location]

        when:
        // Create tables with the same db and tbl name but different catalogs
        createTable(polaris_metastore, db_name_1, tbl_name_1, initial_metadata)
        Thread.sleep(5000)
        createTable(polaris_metastore, db_name_1, tbl_name_2, initial_metadata)
        createTable(polaris_metastore_test, db_name_1, tbl_name_1, initial_metadata)

        then:
        // check table exists
        api.tableExists(polaris_metastore, db_name_1, tbl_name_1)
        api.tableExists(polaris_metastore, db_name_1, tbl_name_2)
        api.tableExists(polaris_metastore_test, db_name_1, tbl_name_1)

        // get the table
        when:
        def polaris_metastore_db1_tbl1 = api.getTable(polaris_metastore, db_name_1, tbl_name_1, true, true, true)
        def polaris_metastore_db1_tbl2 = api.getTable(polaris_metastore, db_name_1, tbl_name_2, true, true, true)
        def polaris_metastore_test_db1_tbl1 = api.getTable(polaris_metastore_test, db_name_1, tbl_name_1, true, true, true)
        then:
        Assertions.assertEquals(polaris_metastore_db1_tbl1.name, QualifiedName.ofTable(polaris_metastore, db_name_1, tbl_name_1))
        Assertions.assertEquals(polaris_metastore_db1_tbl2.name, QualifiedName.ofTable(polaris_metastore, db_name_1, tbl_name_2))
        Assertions.assertEquals(polaris_metastore_test_db1_tbl1.name, QualifiedName.ofTable(polaris_metastore_test, db_name_1, tbl_name_1))

        // assert update by updating polaris_metastore_db1_tbl1
        when:
        polaris_metastore_db1_tbl1.setMetadata(updated_metadata)
        api.updateTable(polaris_metastore, db_name_1, tbl_name_1, polaris_metastore_db1_tbl1)
        polaris_metastore_db1_tbl1 = api.getTable(polaris_metastore, db_name_1, tbl_name_1, true, true, true)
        polaris_metastore_db1_tbl2 = api.getTable(polaris_metastore, db_name_1, tbl_name_2, true, true, true)
        polaris_metastore_test_db1_tbl1 = api.getTable(polaris_metastore_test, db_name_1, tbl_name_1, true, true, true)
        then:
        Assertions.assertEquals(polaris_metastore_db1_tbl1.metadata.get("metadata_location").toString(), updated_metadata_location)
        Assertions.assertEquals(polaris_metastore_db1_tbl2.metadata.get("metadata_location").toString(), metadata_location)
        Assertions.assertEquals(polaris_metastore_test_db1_tbl1.metadata.get("metadata_location").toString(), metadata_location)

        // assert list
        def expected_polaris_metastore_tbls = ["table_1", "table_2"] as Set
        def expected_polaris_metastore_test_tbls = ["table_1"] as Set
        when:
        Thread.sleep(5000)
        then:
        Assertions.assertEquals(
            api.getDatabase(polaris_metastore, db_name_1, true, true).tables.toSet(),
            expected_polaris_metastore_tbls
        )
        Assertions.assertEquals(
            api.getDatabase(polaris_metastore_test, db_name_1, true, true).tables.toSet(),
            expected_polaris_metastore_test_tbls
        )

        // assert rename
        when:
        api.renameTable(polaris_metastore, db_name_1, tbl_name_1, new_tbl_name_1)
        then:
        api.tableExists(polaris_metastore, db_name_1, new_tbl_name_1)
        api.tableExists(polaris_metastore, db_name_1, tbl_name_2)
        api.tableExists(polaris_metastore_test, db_name_1, tbl_name_1)

        when:
        api.tableExists(polaris_metastore, db_name_1, tbl_name_1)
        then:
        thrown(Exception)

        // assert delete
        api.deleteTable(polaris_metastore, db_name_1, new_tbl_name_1)
        api.deleteTable(polaris_metastore, db_name_1, tbl_name_2)
        api.deleteTable(polaris_metastore_test, db_name_1, tbl_name_1)
        api.deleteDatabase(polaris_metastore, db_name_1)
        api.deleteDatabase(polaris_metastore_test, db_name_1)
    }
}
