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

import com.netflix.metacat.client.Client
import com.netflix.metacat.client.api.MetacatV1
import com.netflix.metacat.client.api.MetadataV1
import com.netflix.metacat.client.api.PartitionV1
import com.netflix.metacat.client.api.TagV1
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException
import com.netflix.metacat.common.exception.MetacatBadRequestException
import com.netflix.metacat.common.exception.MetacatNotFoundException
import com.netflix.metacat.common.exception.MetacatNotSupportedException
import com.netflix.metacat.common.exception.MetacatPreconditionFailedException
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import com.netflix.metacat.connector.hive.util.PartitionUtil
import com.netflix.metacat.testdata.provider.PigDataDtoProvider
import groovy.sql.Sql
import org.apache.commons.io.FileUtils
import org.joda.time.Instant
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
    public static MetacatJson metacatJson = new MetacatJsonLocator()

    def setupSpec() {
        String url = "http://localhost:${System.properties['metacat_http_port']}"
        assert url, 'Required system property "metacat_url" is not set'

        def client = Client.builder()
            .withHost(url)
            .withUserName('metacat-test')
            .withClientAppName('metacat-test')
            .build()
        def hiveClient = Client.builder()
            .withHost(url)
            .withDataTypeContext('hive')
            .withUserName('metacat-test')
            .withClientAppName('metacat-test')
            .build()
        hiveContextApi = hiveClient.api
        api = client.api
        partitionApi = client.partitionApi
        tagApi = client.tagApi
        metadataApi = client.metadataApi
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
            } else {
                newTable = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, owner, uri)
            }
            if (metadata != null) {
                newTable.getMetadata() == null? newTable.setMetadata(metadata): newTable.getMetadata().putAll(metadata)
            }
            api.createTable(catalogName, databaseName, tableName, newTable)
        }
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
        'embedded-hive-metastore'       | 'smoke_db0'  | 'file:/tmp/embedded-hive-metastore/smoke_db00'       | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db0' | 'file:/tmp/embedded-fast-hive-metastore/fsmoke_db00' | null
        'embedded-fast-hive-metastore'  | 'shard1'     | null                                                 | null
        'hive-metastore'                | 'hsmoke_db0' | 'file:/tmp/hive-metastore/hsmoke_db00'               | null
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
        'embedded-hive-metastore'       | 'smoke_db0'  | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db0' | 'file:/tmp/embedded-fast-hive-metastore/fsmoke_db00'
        'embedded-fast-hive-metastore'  | 'shard1'     | null
        'hive-metastore'                | 'hsmoke_db0' | null
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
            table = api.getTable(catalogName, databaseName, tableName, true, true, false)
            assert table.getFields().get(0).type == 'chararray'
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
        'embedded-hive-metastore'       | 'smoke_db1'  | 'test_create_table' | true   | false   | null
        'embedded-hive-metastore'       | 'smoke_db1'  | 'test_create_table' | false  | false   | null
        'embedded-hive-metastore'       | 'smoke_db1'  | 'test_create_table1'| false  | true    | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db1' | 'test_create_table' | true   | false   | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db1' | 'test_create_table' | false  | false   | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db1' | 'test_create_table1'| false  | true    | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db1' | 'test.create_table1'| false  | true    | InvalidMetaException.class
        'embedded-fast-hive-metastore'  | 'shard'      | 'test_create_table' | true   | false   | null
        'embedded-fast-hive-metastore'  | 'shard'      | 'test_create_table' | false  | false   | null
        'embedded-fast-hive-metastore'  | 'shard'      | 'test_create_table1'| false  | true    | null
        'embedded-fast-hive-metastore'  | 'shard'      | 'test.create_table1'| false  | true    | InvalidMetaException.class
        'hive-metastore'                | 'hsmoke_db1' | 'test_create_table' | true   | false   | null
        'hive-metastore'                | 'hsmoke_db1' | 'test_create_table' | false  | false   | null
        'hive-metastore'                | 'hsmoke_db1' | 'test_create_table1'| false  | true    | null
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table' | true   | false   | null
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table.definition' | true   | false   | null   //verify the table name can have . character
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table.knp' | true   | false   | null   //verify the table name can have knp extension
        's3-mysql-db'                   | 'smoke_db1'  | 'test_create_table.mp3' | true   | false   | null   //verify the table name can have knp extension
        'invalid-catalog'               | 'smoke_db1'  | 'z'                 | true   | false   | MetacatNotFoundException.class
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
        'embedded-fast-hive-metastore'  | 'fsmoke_db2' | 'test_create_table' | true   | false   | null
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
        'embedded-fast-hive-metastore'  | 'fsmoke_acl' | 'test_create_table'
    }

    def "Test get table names"() {
        given:
        def catalogName = 'embedded-fast-hive-metastore'
        def databaseName = 'fsmoke_db_names'
        def database1Name = 'fsmoke_db1_names'
        def database2Name = 'fsmoke_db2_names'
        def tableName = 'fsmoke_table_names'
        def ownerFilter = 'hive_filter_field_owner__= "amajumdar"'
        def paramFilter = 'hive_filter_field_params__type = "ice"'
        def ownerParamFilter = 'hive_filter_field_params__type = "ice" and hive_filter_field_owner__= "amajumdar"'
        IntStream.range(0,15).forEach{ i -> createTable(catalogName, databaseName, tableName + i)}
        IntStream.range(0,25).forEach{ i -> createTable(catalogName, database1Name, tableName + i)}
        IntStream.range(0,10).forEach{ i -> createTable(catalogName, database2Name, tableName + i, ['type':'ice'])}
        when:
        api.getTableNames(catalogName, null, 10)
        then:
        thrown(MetacatBadRequestException)
        when:
        def result = api.getTableNames(catalogName, ownerFilter, 10)
        then:
        result.size() == 10
        when:
        result = api.getTableNames(catalogName, ownerFilter, 100)
        then:
        result.size() >= 50
        when:
        result = api.getTableNames(catalogName, databaseName, ownerFilter, 10)
        then:
        result.size() == 10
        when:
        result = api.getTableNames(catalogName, databaseName, ownerFilter, 100)
        then:
        result.size() == 15
        when:
        result = api.getTableNames(catalogName, database2Name, paramFilter, 100)
        then:
        result.size() == 10
        when:
        result = api.getTableNames(catalogName, database2Name, ownerParamFilter, 100)
        then:
        result.size() == 10
        when:
        result = api.getTableNames(catalogName, ownerParamFilter, 100)
        then:
        result.size() == 10
        cleanup:
        IntStream.range(0,15).forEach{ i -> api.deleteTable(catalogName, databaseName, tableName + i)}
        IntStream.range(0,25).forEach{ i -> api.deleteTable(catalogName, database1Name, tableName + i)}
        IntStream.range(0,10).forEach{ i -> api.deleteTable(catalogName, database2Name, tableName + i)}
    }

    @Unroll
    def "Test create/update iceberg table"() {
        given:
        def catalogName = 'embedded-fast-hive-metastore'
        def databaseName = 'iceberg_db'
        def tableName = 'iceberg_table'
        def renamedTableName = 'iceberg_table_rename'
        def icebergManifestFileName = '/metacat-test-cluster/etc-metacat/data/icebergManifest.json'
        def metadataFileName = '/metacat-test-cluster/etc-metacat/data/00088-5641e8bf-06b8-46b3-a0fc-5c867f5bca58.metadata.json'
        def curWorkingDir = new File("").getAbsolutePath()
        def icebergManifestFile = new File(curWorkingDir + icebergManifestFileName)
        def metadataFile = new File(curWorkingDir + metadataFileName)
        def uri = isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : null
        def tableDto = PigDataDtoProvider.getTable(catalogName, databaseName, tableName, 'test', uri)
        tableDto.setFields([])
        def metadataLocation = '/tmp/data/00088-5641e8bf-06b8-46b3-a0fc-5c867f5bca58.metadata.json'
        def icebergMetadataLocation = '/tmp/data/icebergManifest.json'
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)
        when:
        // Updating an non-iceberg table with iceberg metadata should fail
        createTable(catalogName, databaseName, tableName)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        thrown(MetacatBadRequestException)
        when:
        api.deleteTable(catalogName, databaseName, tableName)
        api.createTable(catalogName, databaseName, tableName, tableDto)
        def metadataLocation1 = '/tmp/data/00088-5641e8bf-06b8-46b3-a0fc-5c867f5bca58.metadata.json'
        def metadata1 = [table_type: 'ICEBERG', metadata_location: metadataLocation1, previous_metadata_location: metadataLocation]
        def updatedUri = tableDto.getDataUri() + 'updated'
        tableDto.getMetadata().putAll(metadata1)
        tableDto.getSerde().setUri(updatedUri)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        noExceptionThrown()
        when:
        FileUtils.moveFile(metadataFile, new File(metadataFile.getAbsolutePath() + '1'))
        api.getTable(catalogName, databaseName, tableName, true, false, false)
        then:
        thrown(MetacatBadRequestException)
        FileUtils.moveFile(new File(metadataFile.getAbsolutePath() + '1'), metadataFile)
        when:
        def updatedTable = api.getTable(catalogName, databaseName, tableName, true, false, false)
        then:
        noExceptionThrown()
        updatedTable.getMetadata().get('metadata_location') == metadataLocation1
        updatedTable != null
        updatedTable.getDataUri() == updatedUri
        when:
        def metadataLocation2 = '/tmp/data/00089-5641e8bf-06b8-46b3-a0fc-5c867f5bca58.metadata.json'
        def metadata2 = [table_type: 'ICEBERG', metadata_location: metadataLocation2, previous_metadata_location: metadataLocation1, 'partition_spec': 'invalid']
        tableDto.getMetadata().putAll(metadata2)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        updatedTable = api.getTable(catalogName, databaseName, tableName, true, false, false)
        then:
        noExceptionThrown()
        updatedTable.getMetadata().get('metadata_location') == metadataLocation2
        updatedTable.getMetadata().get('partition_spec') != 'invalid'
        !updatedTable.getMetadata().containsKey('metadata_content')
        when:
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        def tableWithDetails = api.getTable(catalogName, databaseName, tableName, true, false, false, true)
        then:
        tableWithDetails.getMetadata().get('metadata_content') != null
        when:
        api.deleteTable(catalogName, databaseName, renamedTableName)
        api.renameTable(catalogName, databaseName, tableName, renamedTableName)
        def t = api.getTable(catalogName, databaseName, renamedTableName, true, false, false)
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
        updatedTable = api.getTable(catalogName, databaseName, tableName, true, false, false)
        updatedTable.getFields().get(0).setComment('new comments')
        api.updateTable(catalogName, databaseName, tableName, updatedTable)
        updatedTable = api.getTable(catalogName, databaseName, tableName, true, false, false)
        then:
        noExceptionThrown()
        updatedTable.getFields().get(0).getComment() == 'new comments'
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
        noExceptionThrown()
        cleanup:
        FileUtils.deleteQuietly(icebergManifestFile)
    }

    @Unroll
    def "Test get iceberg table and partitions"() {
        given:
        def catalogName = 'embedded-fast-hive-metastore'
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

        def metadataLocation = String.format('/tmp/data/00088-5641e8bf-06b8-46b3-a0fc-5c867f5bca58.metadata.json')

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
        partitionApi.getPartitionCount(catalogName, databaseName, tableName)
        then:
        thrown(MetacatNotSupportedException)

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
    def "Test get partitions from iceberg table using #filter"() {
        def catalogName = 'embedded-fast-hive-metastore'
        def databaseName = 'iceberg_db'
        def tableName = 'iceberg_table_6'
        def tableDto = new TableDto(
            name: QualifiedName.ofTable(catalogName, databaseName, tableName),
            metadata: [table_type: 'ICEBERG',
                       metadata_location: String.format('/tmp/data/00088-5641e8bf-06b8-46b3-a0fc-5c867f5bca58.metadata.json')]
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
    def "Test delete table #catalogName/#databaseName/#tableName"() {
        given:
        def name = catalogName + '/' + databaseName + '/' + tableName
        createTable(catalogName, databaseName, tableName)
        def partitions = PigDataDtoProvider.getPartitions(catalogName, databaseName, tableName, 'field1=xyz/field3=abc', isLocalEnv ? 'file:/tmp/abc' : null, count)
        partitionApi.savePartitions(catalogName, databaseName, tableName, new PartitionsSaveRequestDto(partitions: partitions))
        api.deleteTable(catalogName, databaseName, tableName)
        def definitions = metadataApi.getDefinitionMetadataList(null, null, null, null, null, null, name,null)
        expect:
        definitions.size() == result
        cleanup:
        metadataApi.deleteDefinitionMetadata(name, true)
        where:
        catalogName                     | databaseName  | tableName             | count     | result
        'embedded-hive-metastore'       | 'smoke_ddb1'  | 'test_create_table'   | 15        | 0
        'embedded-fast-hive-metastore'  | 'fsmoke_ddb1' | 'test_create_table'   | 15        | 0
        'embedded-fast-hive-metastore'  | 'shard'       | 'test_create_table'   | 15        | 0
        'hive-metastore'                | 'hsmoke_ddb'  | 'test_create_table'   | 15        | 0
        'hive-metastore'                | 'hsmoke_ddb1' | 'test_create_table1'  | 15        | 0
        'hive-metastore'                | 'hsmoke_ddb1' | 'test_create_table2'  | 15        | 1
        's3-mysql-db'                   | 'smoke_ddb1'  | 'test_create_table'   | 15        | 0
        'embedded-hive-metastore'       | 'smoke_ddb1'  | 'test_create_table'   | 10        | 0
        'embedded-fast-hive-metastore'  | 'fsmoke_ddb1' | 'test_create_table'   | 10        | 0
        'embedded-fast-hive-metastore'  | 'shard'       | 'test_create_table'   | 10        | 0
        'hive-metastore'                | 'hsmoke_ddb'  | 'test_create_table'   | 10        | 0
        'hive-metastore'                | 'hsmoke_ddb1' | 'test_create_table1'  | 10        | 0
        'hive-metastore'                | 'hsmoke_ddb1' | 'test_create_table2'  | 10        | 1
        's3-mysql-db'                   | 'smoke_ddb1'  | 'test_create_table'   | 10        | 0
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
        'embedded-fast-hive-metastore'  | 'shard'
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
        'embedded-fast-hive-metastore'  | 'shard'      | 'part'
        'hive-metastore'                | 'hsmoke_db2' | 'part'
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
        table.getMetadata() != null && table.getMetadata().get('EXTERNAL').equalsIgnoreCase('TRUE')
        table != null && table.name.tableName == newTableName
        table.getDefinitionMetadata() != null
        cleanup:
        api.deleteTable(catalogName, databaseName, newTableName)
        where:
        catalogName                     | databaseName | tableName            | external | newTableName
        'embedded-hive-metastore'       | 'smoke_db3'  | 'test_create_table'  | null     | 'test_create_table1'
        'embedded-fast-hive-metastore'  | 'fsmoke_db3' | 'test_create_table'  | null     | 'test_create_table1'
        'embedded-fast-hive-metastore'  | 'shard'      | 'test_create_table'  | null     | 'test_create_table1'
        'embedded-fast-hive-metastore'  | 'shard'      | 'test_create_tablet' | 'TRUE'   | 'test_create_tablet1'
        'embedded-fast-hive-metastore'  | 'shard'      | 'test_create_tablef' | 'FALSE'  | 'test_create_tablef1'
        'hive-metastore'                | 'hsmoke_db3' | 'test_create_table'  | null     | 'test_create_table1'
    }

    @Unroll
    def "Test drop and rename table"() {
        given:
        def tableName = 'test_create_table_rename'
        def tableName1 = 'test_create_table_rename1'
        createTable(catalogName, databaseName, tableName, 'TRUE')
        createTable(catalogName, databaseName, tableName1, 'TRUE')
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
        table.getMetadata() != null && table.getMetadata().get('EXTERNAL').equalsIgnoreCase('TRUE')
        table.getDefinitionMetadata() != null
        cleanup:
        api.deleteTable(catalogName, databaseName, tableName1)
        where:
        catalogName                    | databaseName
        'embedded-fast-hive-metastore' | 'hsmoke_db3'
        'embedded-fast-hive-metastore' | 'fsmoke_ddb1'
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
        'embedded-fast-hive-metastore'  | 'shard'      | 'part'              | 'part_view' | null                           | false
        'embedded-fast-hive-metastore'  | 'shard'      | 'part'              | 'part_view' | null                           | true
        'hive-metastore'                | 'hsmoke_db4' | 'part'              | 'part_view' | null                           | false
        'hive-metastore'                | 'hsmoke_db4' | 'part'              | 'part_view' | null                           | true
        'embedded-hive-metastore'       | 'smoke_db4'  | 'metacat_all_types' | 'part_view' | null                           | false
        'embedded-fast-hive-metastore'  | 'fsmoke_db4' | 'metacat_all_types' | 'part_view' | null                           | false
        'embedded-fast-hive-metastore'  | 'shard'      | 'metacat_all_types' | 'part_view' | null                           | false
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
        'embedded-fast-hive-metastore'  | 'shard'        | 'part'              | 'part_view' | null                           | false
        'embedded-fast-hive-metastore'  | 'shard'        | 'part'              | 'part_view' | null                           | true
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
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'
        'hive-metastore'                | 'hsmoke_db'       | 'part'
        's3-mysql-db'                   | 'smoke_db'        | 'part'
    }

    @Unroll
    def "Test get AUDIT table partitions for #catalogName/#databaseName/#tableName"() {
        def partRequestDto = new PartitionsSaveRequestDto(
            definitionMetadata: metacatJson.emptyObjectNode(),
            partitions: [
                new PartitionDto(
                    name: QualifiedName.ofPartition(catalogName, databaseName, tableName, "one=1"),
                    definitionMetadata: metacatJson.emptyObjectNode(),
                    dataMetadata: metacatJson.emptyObjectNode(),
                    dataExternal: true,
                    audit: new AuditDto(
                        createdDate: Instant.now().toDate(),
                        lastModifiedDate: Instant.now().toDate()
                    ),
                    serde: new StorageDto(
                        inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                        outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        uri: "file://tmp/file/"
                    ),
                )
            ]
        )

        def partRequestDto2 = new PartitionsSaveRequestDto(
            definitionMetadata: metacatJson.emptyObjectNode(),
            partitions: [
                new PartitionDto(
                    name: QualifiedName.ofPartition(catalogName, databaseName, tableName, "one=2"),
                    definitionMetadata: metacatJson.emptyObjectNode(),
                    dataMetadata: metacatJson.emptyObjectNode(),
                    dataExternal: true,
                    audit: new AuditDto(
                        createdDate: Instant.now().toDate(),
                        lastModifiedDate: Instant.now().toDate()
                    ),
                    serde: new StorageDto(
                        inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                        outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        uri: "file://tmp/file2/"
                    ),
                )
            ]
        )
        when:
        createTable(catalogName, databaseName, tableName)

        partitionApi.savePartitions(catalogName, databaseName, tableName, partRequestDto)
        if ( !tableName.contains('audit')) {
            partitionApi.savePartitions(catalogName, databaseName, tableName, partRequestDto2)
        }

        def partkeys = partitionApi.getPartitionKeys(catalogName, databaseName, tableName,null, null, null, null, null)
        def partkeys2 = partitionApi.getPartitionKeysForRequest(catalogName, databaseName, tableName,null, null, null, null,
            new GetPartitionsRequestDto(null, null, null, null))

        def partUris = partitionApi.getPartitionUris(catalogName, databaseName, tableName,null, null, null, null, null)
        def partUris2 = partitionApi.getPartitionUrisForRequest(catalogName, databaseName, tableName,null, null, null, null,
            new GetPartitionsRequestDto(null, null, null, null))

        def partUris3 = partitionApi.getPartitionsForRequest(catalogName, databaseName, tableName,null, null, null, null,false,
            new GetPartitionsRequestDto(null, null, null, null))

        //test the includeAuditOnly flag
        def auditparts = partitionApi.getPartitionsForRequest(catalogName, databaseName, tableName,null, null, null, null,false,
            new GetPartitionsRequestDto(includeAuditOnly: true))

        def auditpartkeys = partitionApi.getPartitionKeysForRequest(catalogName, databaseName, tableName,null, null, null, null,
            new GetPartitionsRequestDto(includeAuditOnly: true))

        def auditpartUris = partitionApi.getPartitionUrisForRequest(catalogName, databaseName, tableName,null, null, null, null,
            new GetPartitionsRequestDto(includeAuditOnly: true))
        then:
        noExceptionThrown()
        assert partkeys.size() == 2
        assert partUris.size() == partkeys.size()
        assert partUris2.size() == partkeys.size()
        assert partUris3.size() == partkeys.size() //test the includeAuditOnly and includepartionDetails can be null

        assert partkeys.get(0).equals("one=1")
        assert partkeys2.size() == partkeys.size()
        assert auditparts.size() == auditOnlyPartSize
        assert auditpartkeys.size() == auditOnlyPartSize
        assert auditpartUris.size() == auditOnlyPartSize

        where:
        catalogName                     | databaseName      | tableName                       | auditOnlyPartSize
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'                          | 2
        'embedded-fast-hive-metastore'  | 'audit'           | 'fsmoke_db__part__audit_12345'  | 1
    }

    @Unroll
    def "Test('#repeat') save partitions for #catalogName/#databaseName/#tableName with partition name starting with #partitionName"() {
        given:
        def escapedPartitionName = PartitionUtil.escapePartitionName(partitionName)
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
                request.setDefinitionMetadata(metacatJson.emptyObjectNode().set('savePartitions', metacatJson.emptyObjectNode()))
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
            def partitions = partitionApi.getPartitions(catalogName, databaseName, tableName, escapedPartitionName.replace('=', '="') + '"', null, null, null, null, true)
            assert partitions != null && partitions.size() == 1 && partitions.find {
                it.name.partitionName == escapedPartitionName
            } != null
            def partitionDetails = partitionApi.getPartitionsForRequest(catalogName, databaseName, tableName, null, null, null, null, true, new GetPartitionsRequestDto(filter: escapedPartitionName.replace('=', '="') + '"', includePartitionDetails: true))
            def partitionDetail = partitionDetails.find { it.name.partitionName == escapedPartitionName }
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
            partitionApi.deletePartitions(catalogName, databaseName, tableName, [escapedPartitionName])
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
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | "one=xy'z"    | ''        | ''        | false  | false | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | "one=xy'z"    | ''        | ''        | false  | true  | null
        'embedded-fast-hive-metastore'  | 'fsmoke_db'       | 'part'    | 'two=xyz'     | ''        | ''        | false  | false | MetacatBadRequestException.class
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'one=xyz'     | ''        | ''        | false  | false | null
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'one=xyz'     | '/'       | ''        | false  | false | null
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'one=xyz'     | ''        | ''        | true   | false | null
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'one=xyz'     | '/'       | ''        | true   | false | null
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'one=xyz'     | ''        | ''        | true   | true  | null
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'one=xyz'     | '/'       | ''        | true   | true  | null
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'two=xyz'     | ''        | ''        | false  | false | MetacatBadRequestException.class
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
    def "Test save partitions for #catalogName/#databaseName/#tableName with serde set to null: #serdeNull and location set to null:#locationNull"() {
        given:
        def uri = isLocalEnv ? String.format("file:/tmp/%s/%s/%s", databaseName, tableName, partitionName) : String.format("s3://wh/%s/%s/%s", databaseName, tableName, partitionName)
        createTable(catalogName, databaseName, tableName)
        def partition = PigDataDtoProvider.getPartition(catalogName, databaseName, tableName, partitionName, uri)
        if (locationNull) {
            partition.getSerde().setUri(null)
        }
        if (serdeNull) {
            partition.setSerde(null)
        }
        def request = new PartitionsSaveRequestDto(partitions: [partition])
        partitionApi.savePartitions(catalogName, databaseName, tableName, request)
        def partitions = partitionApi.getPartitions(catalogName, databaseName, tableName, partitionName.replace('=', '="') + '"', null, null, null, null, false)
        expect:
        partitions.get(0).dataUri == uri
        cleanup:
        partitionApi.deletePartitions(catalogName, databaseName, tableName, [partitionName])
        where:
        catalogName                     | databaseName      | tableName | partitionName | serdeNull | locationNull
        'embedded-hive-metastore'       | 'smoke_db7'       | 'part'    | 'one=xyz'     | true      | true
        'embedded-hive-metastore'       | 'smoke_db7'       | 'part'    | 'one=xyz'     | true      | false
        'embedded-hive-metastore'       | 'smoke_db7'       | 'part'    | 'one=xyz'     | false     | true
        'embedded-fast-hive-metastore'  | 'fsmoke_db7'      | 'part'    | 'one=xyz'     | true      | true
        'embedded-fast-hive-metastore'  | 'fsmoke_db7'      | 'part'    | 'one=xyz'     | true      | false
        'embedded-fast-hive-metastore'  | 'fsmoke_db7'      | 'part'    | 'one=xyz'     | false     | true
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'one=xyz'     | true      | true
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'one=xyz'     | true      | false
        'embedded-fast-hive-metastore'  | 'shard'           | 'part'    | 'one=xyz'     | false     | true
        'hive-metastore'                | 'hsmoke_db7'      | 'part'    | 'one=xyz'     | true      | true
        'hive-metastore'                | 'hsmoke_db7'      | 'part'    | 'one=xyz'     | true      | false
        'hive-metastore'                | 'hsmoke_db7'      | 'part'    | 'one=xyz'     | false     | true
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
        'embedded-hive-metastore'       | 'smoke_db5'  | 'part'    | 'one=xyz'     | 15    | 0
        'embedded-hive-metastore'       | 'smoke_db5'  | 'part'    | 'one=xyz'     | 15    | 15
        'embedded-hive-metastore'       | 'smoke_db5'  | 'part'    | 'one=xyz'     | 15    | 5
        'embedded-fast-hive-metastore'  | 'fsmoke_db5' | 'part'    | 'one=xyz'     | 15    | 0
        'embedded-fast-hive-metastore'  | 'fsmoke_db5' | 'part'    | 'one=xyz'     | 15    | 15
        'embedded-fast-hive-metastore'  | 'fsmoke_db5' | 'part'    | 'one=xyz'     | 15    | 5
        'hive-metastore'                | 'hsmoke_db5' | 'part'    | 'one=xyz'     | 15    | 0
        'hive-metastore'                | 'hsmoke_db5' | 'part'    | 'one=xyz'     | 15    | 15
        'hive-metastore'                | 'hsmoke_db5' | 'part'    | 'one=xyz'     | 15    | 5
    }

    @Unroll
    def "Test Get partitions threshold"() {
        given:
        def catalogName = 'embedded-fast-hive-metastore'
        def databaseName = 'fsmoke_db5'
        def tableName = 'part'
        def request = new PartitionsSaveRequestDto()
        def uri = isLocalEnv ? 'file:/tmp/abc' : null
        createTable(catalogName, databaseName, tableName)
        def partitions = PigDataDtoProvider.getPartitions(catalogName, databaseName, tableName, 'one=xyz', uri, 110)
        request.setPartitions(partitions)
        partitionApi.savePartitions(catalogName, databaseName, tableName, request)
        when:
        def savedPartitions = partitionApi.getPartitions(catalogName, databaseName, tableName, null, null, null, null, 10, true)
        then:
        noExceptionThrown()
        savedPartitions != null && savedPartitions.size() == 10
        when:
        savedPartitions = partitionApi.getPartitions(catalogName, databaseName, tableName, null, null, null, null, 100, true)
        then:
        noExceptionThrown()
        savedPartitions != null && savedPartitions.size() == 100
        when:
        partitionApi.getPartitions(catalogName, databaseName, tableName, null, null, null, null, 101, true)
        then:
        thrown(Exception)
        cleanup:
        api.deleteTable(catalogName, databaseName, tableName)
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
        'embedded-fast-hive-metastore'  | 'shard'      | 'part'    | ['test'] as Set<String>           | true
        'embedded-fast-hive-metastore'  | 'shard'      | 'part'    | ['test', 'unused'] as Set<String> | false
        'hive-metastore'                | 'hsmoke_db6' | 'part'    | ['test'] as Set<String>           | true
        'hive-metastore'                | 'hsmoke_db6' | 'part'    | ['test', 'unused'] as Set<String> | false
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
        'embedded-hive-metastore'       | 'smoke_db6'  | 'part'    | ['test_tag']    as List<String>        | true
        'embedded-hive-metastore'       | 'smoke_db6'  | 'part'    | ['test_tag', 'unused'] as List<String> | false
        'embedded-fast-hive-metastore'  | 'fsmoke_db6' | 'part'    | ['test_tag'] as List<String>           | true
        'embedded-fast-hive-metastore'  | 'fsmoke_db6' | 'part'    | ['test_tag', 'unused'] as List<String> | false
        'embedded-fast-hive-metastore'  | 'shard'      | 'part'    | ['test_tag'] as List<String>           | true
        'embedded-fast-hive-metastore'  | 'shard'      | 'part'    | ['test_tag', 'unused'] as List<String> | false
        'hive-metastore'                | 'hsmoke_db6' | 'part'    | ['test_tag'] as List<String>           | true
        'hive-metastore'                | 'hsmoke_db6' | 'part'    | ['test_tag', 'unused'] as List<String> | false
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
        catalogName                     | databaseName | tableName           | viewName    |tags
        'embedded-hive-metastore'       | 'smoke_db4'  | 'part'              | 'part_view'  | ['test_tag']    as List<String>
        'embedded-fast-hive-metastore'  | 'fsmoke_db4' | 'part'              | 'part_view' | ['test_tag']    as List<String>
        'embedded-fast-hive-metastore'  | 'shard'      | 'part'              | 'part_view' | ['test_tag']    as List<String>
        'hive-metastore'                | 'hsmoke_db4' | 'part'              | 'part_view' | ['test_tag']    as List<String>
        's3-mysql-db'                   | 'smoke_db4'  | 'part'              | 'part_view' | ['test_tag']    as List<String>
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
