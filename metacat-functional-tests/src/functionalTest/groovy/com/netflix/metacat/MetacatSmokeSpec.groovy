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
import com.netflix.metacat.common.exception.MetacatTooManyRequestsException
import com.netflix.metacat.common.exception.MetacatUnAuthorizedException
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import com.netflix.metacat.common.server.connectors.exception.TableMigrationInProgressException
import com.netflix.metacat.connector.hive.util.PartitionUtil
import com.netflix.metacat.testdata.provider.PigDataDtoProvider
import feign.Logger
import feign.RetryableException
import feign.Retryer
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
            assert api.doesTableExist(catalogName, databaseName, tableName)
            def table = api.getTable(catalogName, databaseName, tableName, true, true, false)
            assert table != null && table.name.tableName == tableName
            assert table.getDefinitionMetadata() != null
            assert table.getSerde().getUri() == updatedUri
            //TODO: assert table.getSerde().getParameters().size() == 0
            assert table.getSerde().getSerdeInfoParameters().size() >= 1
            assert table.getSerde().getSerdeInfoParameters().get('serialization.format') == '1'
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

    def testCreateTableWithOwner() {
        given:
        def catalogName = 'embedded-fast-hive-metastore'
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

    def "Test materialized common view create/drop"() {
        given:
        def catalogName = 'embedded-fast-hive-metastore'
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
        def metadataLocation = '/tmp/data/metadata/00000-9b5d4c36-130c-4288-9599-7d850c203d11.metadata.json'
        def icebergMetadataLocation = '/tmp/data/icebergManifest.json'
        def metadata = [table_type: 'ICEBERG', metadata_location: metadataLocation]
        tableDto.setMetadata(metadata)
        when:
        // Updating a non-iceberg table with iceberg metadata should fail
        createTable(catalogName, databaseName, tableName)
        api.updateTable(catalogName, databaseName, tableName, tableDto)
        then:
        thrown(MetacatBadRequestException)
        when:
        api.deleteTable(catalogName, databaseName, tableName)
        api.createTable(catalogName, databaseName, tableName, tableDto)
        def metadataLocation1 = '/tmp/data/metadata/00001-abf48887-aa4f-4bcc-9219-1e1721314ee1.metadata.json'
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
        FileUtils.moveFile(metadataFile, new File(metadataFile.getAbsolutePath() + '1'))
        api.getTable(catalogName, databaseName, tableName, true, false, false)
        then:
        thrown(MetacatBadRequestException)
        FileUtils.moveFile(new File(metadataFile.getAbsolutePath() + '1'), metadataFile)
        when:
        def updatedTable = api.getTable(catalogName, databaseName, tableName, true, false, false)
        then:
        noExceptionThrown()
        api.doesTableExist(catalogName, databaseName, tableName)
        updatedTable.getMetadata().get('metadata_location') == metadataLocation1
        updatedTable != null
        if (catalogName == 'embedded-fast-hive-metastore') {
            updatedTable.getDataUri() == updatedUri
            updatedTable.getSerde().getInputFormat() == 'org.apache.hadoop.mapred.TextInputFormat'
        }
        when:
        def metadataLocation2 = '/tmp/data/metadata/00002-2d6c1951-31d5-4bea-8edd-e35746b172f3.metadata.json'
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
        metadata1.put('previous_metadata_location', '/tmp/data/metadata/filenotfound')
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
        catalogName << ['polaris-metastore', 'embedded-fast-hive-metastore']
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
        table.getMetadata() != null && table.getMetadata().get('EXTERNAL').equalsIgnoreCase('TRUE')
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
            def partitions = partitionApi.getPartitions(catalogName, databaseName, tableName, partitionName.replace('=', '="') + '"', null, null, null, null, true)
            assert partitions != null && partitions.size() == 1 && partitions.find {
                it.name.partitionName == escapedPartitionName
            } != null
            def partitionDetails = partitionApi.getPartitionsForRequest(catalogName, databaseName, tableName, null, null, null, null, true, new GetPartitionsRequestDto(filter: partitionName.replace('=', '="') + '"', includePartitionDetails: true))
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
        api.createTable(catalogName, 'invalid', 'invalid', new TableDto(
            name: QualifiedName.ofTable('invalid', 'invalid', 'invalid'),
            serde: new StorageDto(owner: 'ssarma')
        ))
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
        api.createTable(catalogName, 'invalid', 'invalid', new TableDto(
            name: QualifiedName.ofTable('invalid', 'invalid', 'invalid'),
            serde: new StorageDto(owner: 'ssarma')
        ))
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
    def "Test: embedded-hive-metastore get partitions for #filter with null partition values and partition names with hyphen"() {
        given:
        if (cursor == 'start') {
            def uri = isLocalEnv ? 'file:/tmp/abc' : null
            createTable('embedded-hive-metastore', 'smoke_db', 'part_hyphen')
            partitionApi.savePartitions('embedded-hive-metastore', 'smoke_db', 'part_hyphen', new PartitionsSaveRequestDto(partitions: PigDataDtoProvider.getPartitions('embedded-hive-metastore', 'smoke_db', 'part_hyphen', 'one-one=xyz/total=1', uri, 10)))
            partitionApi.savePartitions('embedded-hive-metastore', 'smoke_db', 'part_hyphen', new PartitionsSaveRequestDto(partitions: PigDataDtoProvider.getPartitions('embedded-hive-metastore', 'smoke_db', 'part_hyphen', 'one-one=__HIVE_DEFAULT_PARTITION__/total=1', uri, 10)))
            partitionApi.savePartitions('embedded-hive-metastore', 'smoke_db', 'part_hyphen', new PartitionsSaveRequestDto(partitions: [PigDataDtoProvider.getPartition('embedded-hive-metastore', 'smoke_db', 'part_hyphen', 'one-one=xyz/total=__HIVE_DEFAULT_PARTITION__', uri)]))
        }
        def partitionKeys = partitionApi.getPartitionKeys('embedded-hive-metastore', 'smoke_db', 'part_hyphen', filter, null, null, offset, limit)

        expect:
        partitionKeys.size() == result
        cleanup:
        if (cursor == 'end') {
            def partitionKeysToDrop = partitionApi.getPartitionKeys('embedded-hive-metastore', 'smoke_db', 'part_hyphen', null, null, null, null, null)
            partitionApi.deletePartitions('embedded-hive-metastore', 'smoke_db', 'part_hyphen', partitionKeysToDrop)
        }
        where:
        cursor  | filter                                    | offset | limit | result
        'start' | "one-one=='xyz'"                          | null   | null  | 11
        ''      | "one-one is null"                         | null   | null  | 10
        ''      | "total is null"                           | 0      | 10    | 1
        ''      | "one-one is null and total=10"            | 0      | 10    | 1
        ''      | "one-one is null or total=1"              | 0      | 10    | 10
        'end'   | "one-one is null or one-one=='xyz'"       | null   | null  | 21
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
        when:
        partitionApi.getPartitionsForRequest(catalogName, databaseName, tableName, null, null, null, null, false, new GetPartitionsRequestDto(filter: "(one='20091024') OR (one='20091025') OR (one='20091026') OR (one='20091027') OR (one='20091020') OR (one='20091021') OR (one='20091022') OR (one='20091023') OR (one='20140519') OR (one='20120908') OR (one='20140517') OR (one='20120909') OR (one='20140518') OR (one='20120906') OR (one='20120907') OR (one='20120904') OR (one='20120905') OR (one='20120902') OR (one='20140511') OR (one='20160120') OR (one='20120903') OR (one='20140512') OR (one='20160121') OR (one='20120901') OR (one='20140510') OR (one='20140515') OR (one='20140516') OR (one='20140513') OR (one='20140514') OR (one='20160128') OR (one='20160129') OR (one='20160126') OR (one='20160127') OR (one='20091028') OR (one='20160124') OR (one='20091029') OR (one='20160125') OR (one='20160122') OR (one='20160123') OR (one='20091013') OR (one='20091014') OR (one='20091015') OR (one='20091016') OR (one='20091010') OR (one='20091011') OR (one='20160119') OR (one='20091012') OR (one='20140528') OR (one='20140529') OR (one='20140522') OR (one='20140523') OR (one='20160110') OR (one='20140520') OR (one='20140521') OR (one='20140526') OR (one='20140527') OR (one='20140524') OR (one='20140525') OR (one='20160117') OR (one='20160118') OR (one='20160115') OR (one='20160116') OR (one='20091017') OR (one='20160113') OR (one='20091018') OR (one='20160114') OR (one='20091019') OR (one='20160111') OR (one='20160112') OR (one='20140531') OR (one='20140530') OR (one='20091031') OR (one='20091030') OR (one='20160131') OR (one='20160130') OR (one='20181021') OR (one='20181020') OR (one='20181025') OR (one='20181024') OR (one='20181023') OR (one='20181022') OR (one='20181029') OR (one='20181028') OR (one='20181027') OR (one='20181026') OR (one='20181031') OR (one='20181030') OR (one='20120930') OR (one='20181009') OR (one='20181008') OR (one='20160108') OR (one='20160109') OR (one='20120928') OR (one='20120929') OR (one='20120926') OR (one='20120927') OR (one='20120924') OR (one='20120925') OR (one='20120922') OR (one='20120923') OR (one='20120920') OR (one='20120921') OR (one='20160106') OR (one='20181003') OR (one='20160107') OR (one='20181002') OR (one='20160104') OR (one='20181001') OR (one='20160105') OR (one='20160102') OR (one='20181007') OR (one='20160103') OR (one='20181006') OR (one='20181005') OR (one='20160101') OR (one='20181004') OR (one='20181019') OR (one='20140508') OR (one='20140509') OR (one='20120919') OR (one='20140506') OR (one='20140507') OR (one='20120917') OR (one='20120918') OR (one='20120915') OR (one='20120916') OR (one='20120913') OR (one='20120914') OR (one='20140501') OR (one='20120911') OR (one='20120912') OR (one='20140504') OR (one='20181010') OR (one='20120910') OR (one='20140505') OR (one='20140502') OR (one='20140503') OR (one='20181014') OR (one='20181013') OR (one='20181012') OR (one='20181011') OR (one='20181018') OR (one='20181017') OR (one='20181016') OR (one='20181015') OR (one='20130316') OR (one='20110707') OR (one='20130317') OR (one='20110708') OR (one='20130318') OR (one='20110709') OR (one='20130319') OR (one='20130312') OR (one='20130313') OR (one='20130314') OR (one='20130315') OR (one='20130310') OR (one='20130311') OR (one='20110701') OR (one='20110702') OR (one='20110703') OR (one='20110704') OR (one='20110705') OR (one='20110706') OR (one='20130327') OR (one='20130328') OR (one='20130329') OR (one='20130323') OR (one='20130324') OR (one='20130325') OR (one='20130326') OR (one='20130320') OR (one='20130321') OR (one='20130322') OR (one='20130330') OR (one='20080309') OR (one='20130331') OR (one='20080307') OR (one='20080308') OR (one='20080305') OR (one='20080306') OR (one='20080303') OR (one='20080304') OR (one='20080301') OR (one='20080302') OR (one='20080318') OR (one='20080319') OR (one='20080316') OR (one='20080317') OR (one='20080314') OR (one='20190830') OR (one='20080315') OR (one='20080312') OR (one='20080313') OR (one='20190831') OR (one='20080310') OR (one='20080311') OR (one='20151213') OR (one='20151212') OR (one='20151215') OR (one='20151214') OR (one='20151217') OR (one='20151216') OR (one='20151219') OR (one='20151218') OR (one='20080329') OR (one='20080327') OR (one='20080328') OR (one='20080325') OR (one='20151211') OR (one='20080326') OR (one='20151210') OR (one='20080323') OR (one='20190821') OR (one='20080324') OR (one='20190820') OR (one='20080321') OR (one='20190823') OR (one='20080322') OR (one='20190822') OR (one='20190825') OR (one='20080320') OR (one='20190824') OR (one='20190827') OR (one='20190826') OR (one='20190829') OR (one='20190828') OR (one='20151202') OR (one='20151201') OR (one='20151204') OR (one='20151203') OR (one='20151206') OR (one='20151205') OR (one='20151208') OR (one='20151207') OR (one='20190810') OR (one='20190812') OR (one='20190811') OR (one='20080330') OR (one='20190814') OR (one='20080331') OR (one='20190813') OR (one='20110730') OR (one='20190816') OR (one='20110731') OR (one='20190815') OR (one='20190818') OR (one='20151209') OR (one='20190817') OR (one='20190819') OR (one='20110729') OR (one='20091002') OR (one='20091003') OR (one='20091004') OR (one='20091005') OR (one='20091001') OR (one='20151231') OR (one='20151230') OR (one='20190801') OR (one='20190803') OR (one='20190802') OR (one='20190805') OR (one='20110720') OR (one='20190804') OR (one='20110721') OR (one='20190807') OR (one='20110722') OR (one='20190806') OR (one='20110723') OR (one='20190809') OR (one='20110724') OR (one='20190808') OR (one='20110725') OR (one='20091006') OR (one='20110726') OR (one='20091007') OR (one='20110727') OR (one='20091008') OR (one='20110728') OR (one='20091009') OR (one='20130305') OR (one='20110718') OR (one='20151224') OR (one='20130306') OR (one='20110719') OR (one='20151223') OR (one='20130307') OR (one='20151226') OR (one='20130308') OR (one='20151225') OR (one='20130301') OR (one='20151228') OR (one='20130302') OR (one='20151227') OR (one='20130303') OR (one='20130304') OR (one='20151229') OR (one='20151220') OR (one='20151222') OR (one='20151221') OR (one='20110710') OR (one='20110711') OR (one='20110712') OR (one='20110713') OR (one='20130309') OR (one='20110714') OR (one='20110715') OR (one='20110716') OR (one='20110717') OR (one='20100501') OR (one='20100504') OR (one='20100505') OR (one='20100502') OR (one='20100503') OR (one='20100508') OR (one='20100509') OR (one='20100506') OR (one='20100507') OR (one='20120108') OR (one='20120109') OR (one='20120106') OR (one='20120107') OR (one='20120104') OR (one='20120105') OR (one='20120102') OR (one='20120103') OR (one='20120101') OR (one='20070901') OR (one='20090510') OR (one='20070902') OR (one='20070903') OR (one='20070904') OR (one='20070905') OR (one='20070906') OR (one='20070907') OR (one='20070908') OR (one='20070909') OR (one='20090519') OR (one='20090515') OR (one='20090516') OR (one='20090517') OR (one='20090518') OR (one='20090511') OR (one='20090512') OR (one='20090513') OR (one='20090514') OR (one='20180630') OR (one='20070910') OR (one='20070911') OR (one='20070912') OR (one='20070913') OR (one='20070914') OR (one='20070915') OR (one='20070916') OR (one='20070917') OR (one='20070918') OR (one='20070919') OR (one='20090508') OR (one='20090509') OR (one='20090504') OR (one='20090505') OR (one='20090506') OR (one='20090507') OR (one='20090501') OR (one='20090502') OR (one='20090503') OR (one='20070920') OR (one='20180615') OR (one='20070921') OR (one='20090530') OR (one='20180614') OR (one='20070922') OR (one='20090531') OR (one='20180613') OR (one='20070923') OR (one='20180612') OR (one='20070924') OR (one='20180619') OR (one='20070925') OR (one='20180618') OR (one='20070926') OR (one='20180617') OR (one='20070927') OR (one='20180616') OR (one='20070928') OR (one='20070929') OR (one='20141007') OR (one='20141006') OR (one='20141005') OR (one='20141004') OR (one='20141009') OR (one='20141008') OR (one='20141003') OR (one='20180611') OR (one='20141002') OR (one='20180610') OR (one='20141001') OR (one='20180626') OR (one='20100530') OR (one='20180625') OR (one='20090520') OR (one='20180624') OR (one='20090521') OR (one='20180623') OR (one='20180629') OR (one='20100531') OR (one='20180628') OR (one='20180627') OR (one='20141018') OR (one='20141017') OR (one='20141016') OR (one='20141015') OR (one='20141019') OR (one='20141010') OR (one='20090526') OR (one='20090527') OR (one='20090528') OR (one='20090529') OR (one='20120131') OR (one='20141014') OR (one='20090522') OR (one='20180622') OR (one='20141013') OR (one='20090523') OR (one='20180621') OR (one='20141012') OR (one='20090524') OR (one='20180620') OR (one='20120130') OR (one='20141011') OR (one='20070930') OR (one='20090525') OR (one='20100522') OR (one='20100523') OR (one='20100520') OR (one='20100521') OR (one='20100526') OR (one='20100527') OR (one='20100524') OR (one='20100525') OR (one='20100528') OR (one='20100529') OR (one='20141029') OR (one='20141028') OR (one='20141027') OR (one='20141026') OR (one='20120128') OR (one='20120129') OR (one='20120126') OR (one='20120127') OR (one='20120124') OR (one='20141021') OR (one='20120125') OR (one='20141020') OR (one='20120122') OR (one='20120123') OR (one='20120120') OR (one='20141025') OR (one='20120121') OR (one='20141024') OR (one='20141023') OR (one='20141022') OR (one='20180604') OR (one='20180603') OR (one='20180602') OR (one='20180601') OR (one='20100511') OR (one='20180608') OR (one='20100512') OR (one='20180607') OR (one='20180606') OR (one='20100510') OR (one='20180605') OR (one='20100515') OR (one='20100516') OR (one='20100513') OR (one='20100514') OR (one='20180609') OR (one='20100519') OR (one='20100517') OR (one='20100518') OR (one='20120119') OR (one='20120117') OR (one='20120118') OR (one='20120115') OR (one='20120116') OR (one='20120113') OR (one='20120114') OR (one='20141031') OR (one='20120111') OR (one='20141030') OR (one='20120112') OR (one='20120110') OR (one='20181109') OR (one='20181108') OR (one='20181107') OR (one='20111230') OR (one='20111231') OR (one='20181102') OR (one='20181101') OR (one='20181106') OR (one='20181105') OR (one='20181104') OR (one='20181103') OR (one='20181119') OR (one='20150830') OR (one='20181118') OR (one='20150831') OR (one='20111220') OR (one='20111221') OR (one='20111222') OR (one='20111223') OR (one='20111224') OR (one='20111225') OR (one='20111226') OR (one='20111227') OR (one='20181113') OR (one='20111228') OR (one='20181112') OR (one='20111229') OR (one='20181111') OR (one='20181110') OR (one='20181117') OR (one='20181116') OR (one='20181115') OR (one='20181114') OR (one='20170430') OR (one='20111210') OR (one='20111211') OR (one='20111212') OR (one='20111213') OR (one='20111214') OR (one='20111215') OR (one='20111216') OR (one='20111217') OR (one='20111218') OR (one='20111219') OR (one='20111201') OR (one='20111202') " +
            "OR (one='20111203') OR (one='20111204') OR (one='20111205') OR (one='20111206') OR (one='20111207') OR (one='20111208') OR (one='20111209') OR (one='20150801') OR (one='20170412') OR (one='20170411') OR (one='20170414') OR (one='20170413') OR (one='20170410') OR (one='20170419') OR (one='20170416') OR (one='20170415') OR (one='20170418') OR (one='20170417') OR (one='20150802') OR (one='20150803') OR (one='20150804') OR (one='20150805') OR (one='20150806') OR (one='20150807') OR (one='20150808') OR (one='20150809') OR (one='20170423') OR (one='20170422') OR (one='20170425') OR (one='20170424') OR (one='20170421') OR (one='20170420') OR (one='20170427') OR (one='20170426') OR (one='20170429') OR (one='20170428') OR (one='20150820') OR (one='20150821') OR (one='20150822') OR (one='20150823') OR (one='20150824') OR (one='20150825') OR (one='20150826') OR (one='20150827') OR (one='20150828') OR (one='20150829') OR (one='20150810') OR (one='20150811') OR (one='20150812') OR (one='20170401') OR (one='20170403') OR (one='20170402') OR (one='20170409') OR (one='20170408') OR (one='20170405') OR (one='20170404') OR (one='20170407') OR (one='20170406') OR (one='20150813') OR (one='20150814') OR (one='20150815') OR (one='20150816') OR (one='20150817') OR (one='20150818') OR (one='20150819') OR (one='20140630') OR (one='20190902') OR (one='20190901') OR (one='20190904') OR (one='20190903') OR (one='20190906') OR (one='20190905') OR (one='20190908') OR (one='20190907') OR (one='20190909') OR (one='20101024') OR (one='20101025') OR (one='20101022') OR (one='20101023') OR (one='20101028') OR (one='20101029') OR (one='20101026') OR (one='20101027') OR (one='20101031') OR (one='20101030') OR (one='20101013') OR (one='20101014') OR (one='20101011') OR (one='20101012') OR (one='20101017') OR (one='20101018') OR (one='20101015') OR (one='20101016') OR (one='20101019') OR (one='20101020') OR (one='20101021') OR (one='20101002') OR (one='20101003') OR (one='20101001') OR (one='20101006') OR (one='20101007') OR (one='20101004') OR (one='20101005') OR (one='20101008') OR (one='20101009') OR (one='20101010') OR (one='20160209') OR (one='20160207') OR (one='20160208') OR (one='20160205') OR (one='20160206') OR (one='20160203') OR (one='20160204') OR (one='20160201') OR (one='20160202') OR (one='20140607') OR (one='20140608') OR (one='20140605') OR (one='20140606') OR (one='20140609') OR (one='20140603') OR (one='20140604') OR (one='20140601') OR (one='20140602') OR (one='20181129') OR (one='20160229') OR (one='20140618') OR (one='20140619') OR (one='20140616') OR (one='20140617') OR (one='20140610') OR (one='20140611') OR (one='20160220') OR (one='20140614') OR (one='20181120') OR (one='20140615') OR (one='20140612') OR (one='20140613') OR (one='20160227') OR (one='20181124') OR (one='20160228') OR (one='20181123') OR (one='20160225') OR (one='20181122') OR (one='20160226') OR (one='20181121') OR (one='20160223') OR (one='20181128') OR (one='20160224') OR (one='20181127') OR (one='20160221') OR (one='20181126') OR (one='20160222') OR (one='20181125') OR (one='20160218') OR (one='20160219') OR (one='20140629') OR (one='20140627') OR (one='20140628') OR (one='20140621') OR (one='20140622') OR (one='20140620') OR (one='20140625') OR (one='20140626') OR (one='20181130') OR (one='20140623') OR (one='20140624') OR (one='20160216') OR (one='20160217') OR (one='20160214') OR (one='20160215') OR (one='20160212') OR (one='20160213') OR (one='20160210') OR (one='20160211') OR (one='20110828') OR (one='20110829') OR (one='20130430') OR (one='20110820') OR (one='20110821') OR (one='20110822') OR (one='20110823') OR (one='20110824') OR (one='20110825') OR (one='20110826') OR (one='20110827') OR (one='20110817') OR (one='20180703') OR (one='20110818') OR (one='20180702') OR (one='20110819') OR (one='20180701') OR (one='20180707') OR (one='20180706') OR (one='20180705') OR (one='20180704') OR (one='20180709') OR (one='20180708') OR (one='20110810') OR (one='20110811') OR (one='20110812') OR (one='20110813') OR (one='20110814') OR (one='20110815') OR (one='20110816') OR (one='20110806') OR (one='20110807') OR (one='20110808') OR (one='20110809') OR (one='20141106') OR (one='20141105') OR (one='20141104') OR (one='20141103') OR (one='20141109') OR (one='20141108') OR (one='20141107') OR (one='20110801') OR (one='20110802') OR (one='20141102') OR (one='20110803') OR (one='20141101') OR (one='20110804') OR (one='20110805') OR (one='20141117') OR (one='20141116') OR (one='20141115') OR (one='20141114') OR (one='20141119') OR (one='20141118') OR (one='20141113') OR (one='20141112') OR (one='20141111') OR (one='20141110') OR (one='20080208') OR (one='20080209') OR (one='20080206') OR (one='20080207') OR (one='20080204') OR (one='20080205') OR (one='20080202') OR (one='20080203') OR (one='20080201') OR (one='20130404') OR (one='20130405') OR (one='20130406') OR (one='20130407') OR (one='20130401') OR (one='20130402') OR (one='20130403') OR (one='20080219') OR (one='20080217') OR (one='20080218') OR (one='20080215') OR (one='20080216') OR (one='20080213') OR (one='20080214') OR (one='20190930') OR (one='20080211') OR (one='20080212') OR (one='20080210') OR (one='20130408') OR (one='20130409') OR (one='20130415') OR (one='20130416') OR (one='20130417') OR (one='20130418') OR (one='20130411') OR (one='20130412') OR (one='20130413') OR (one='20130414') OR (one='20130410') OR (one='20080228') OR (one='20080229') OR (one='20080226') OR (one='20080227') OR (one='20080224') OR (one='20190920') OR (one='20080225') OR (one='20080222') OR (one='20190922') OR (one='20080223') OR (one='20190921') OR (one='20080220') OR (one='20190924') OR (one='20080221') OR (one='20190923') OR (one='20190926') OR (one='20190925') OR (one='20190928') OR (one='20190927') OR (one='20190929') OR (one='20130419') OR (one='20130426') OR (one='20130427') OR (one='20130428') OR (one='20130429') OR (one='20130422') OR (one='20130423') OR (one='20130424') OR (one='20130425') OR (one='20130420') OR (one='20130421') OR (one='20190911') OR (one='20190910') OR (one='20190913') OR (one='20190912') OR (one='20190915') OR (one='20110830') OR (one='20190914') OR (one='20110831') OR (one='20190917') OR (one='20190916') OR (one='20190919') OR (one='20190918') OR (one='20100621') OR (one='20100622') OR (one='20100620') OR (one='20100625') OR (one='20100626') OR (one='20100623') OR (one='20100624') OR (one='20100629') OR (one='20190102') OR (one='20190101') OR (one='20100627') OR (one='20190104') OR (one='20100628') OR (one='20190103') OR (one='20190106') OR (one='20190105') OR (one='20120229') OR (one='20190108') OR (one='20190107') OR (one='20120227') OR (one='20120228') OR (one='20190109') OR (one='20120225') OR (one='20120226') OR (one='20120223') OR (one='20120224') OR (one='20120221') OR (one='20120222') OR (one='20120220') OR (one='20100610') OR (one='20100611') OR (one='20100614') OR (one='20100615') OR (one='20100612') OR (one='20170502') OR (one='20100613') OR (one='20170501') OR (one='20100618') OR (one='20100619') OR (one='20100616') OR (one='20100617') OR (one='20170508') OR (one='20170507') OR (one='20120218') OR (one='20120219') OR (one='20170509') OR (one='20120216') OR (one='20170504') OR (one='20120217') OR (one='20170503') OR (one='20120214') OR (one='20170506') OR (one='20120215') OR (one='20170505') OR (one='20120212') OR (one='20120213') OR (one='20120210') OR (one='20120211') OR (one='20100603') OR (one='20100604') OR (one='20100601') OR (one='20100602') OR (one='20100607') OR (one='20100608') OR (one='20100605') OR (one='20100606') OR (one='20120209') OR (one='20150909') OR (one='20120207') OR (one='20100609') OR (one='20120208') OR (one='20120205') OR (one='20120206') OR (one='20120203') OR (one='20120204') OR (one='20120201') OR (one='20150901') OR (one='20120202') OR (one='20150902') OR (one='20150903') OR (one='20150904') OR (one='20150905') OR (one='20150906') OR (one='20150907') OR (one='20150908') OR (one='20070801') OR (one='20090410') OR (one='20070802') OR (one='20090411') OR (one='20070803') OR (one='20070804') OR (one='20070805') OR (one='20070806') OR (one='20070807') OR (one='20070808') OR (one='20070809') OR (one='20141128') OR (one='20141127') OR (one='20141126') OR (one='20141125') OR (one='20141129') OR (one='20141120') OR (one='20090416') OR (one='20090417') OR (one='20090418') OR (one='20090419') OR (one='20141124') OR (one='20090412') OR (one='20141123') OR (one='20090413') OR (one='20180731') OR (one='20141122') OR (one='20090414') OR (one='20180730') OR (one='20141121') OR (one='20090415') OR (one='20070810') OR (one='20070811') OR (one='20070812') OR (one='20070813') OR (one='20070814') OR (one='20070815') OR (one='20070816') OR (one='20070817') OR (one='20070818') OR (one='20070819') OR (one='20090409') OR (one='20090405') OR (one='20141130') OR (one='20090406') OR (one='20090407') OR (one='20090408') OR (one='20090401') OR (one='20090402') OR (one='20090403') OR (one='20090404') OR (one='20070821') OR (one='20090430') OR (one='20180714') OR (one='20070822') OR (one='20180713') OR (one='20070823') OR (one='20180712') OR (one='20070824') OR (one='20180711') OR (one='20070825') OR (one='20180718') OR (one='20070826') OR (one='20180717') OR (one='20070827') OR (one='20180716') OR (one='20070828') OR (one='20180715') OR (one='20070829') OR (one='20180719') OR (one='20180710') OR (one='20070820') OR (one='20180725') OR (one='20090420') OR (one='20180724') OR (one='20090421') OR (one='20180723') OR (one='20090422') OR (one='20180722') OR (one='20180729') OR (one='20180728') OR (one='20100630') OR (one='20180727') OR (one='20180726') OR (one='20090427') OR (one='20090428') OR (one='20090429') OR (one='20090423') OR (one='20180721') OR (one='20090424') OR (one='20180720') OR (one='20070830') OR (one='20090425') OR (one='20070831') OR (one='20090426') OR (one='20181229') OR (one='20181228') OR (one='20181223') OR (one='20181222') OR (one='20181221') OR (one='20181220') OR (one='20181227') OR (one='20181226') OR (one='20181225') OR (one='20181224') OR (one='20181230') OR (one='20181231') OR (one='20181209') OR (one='20181208') OR (one='20181207') OR (one='20181206') OR (one='20160308') OR (one='20160309') OR (one='20160306') OR (one='20160307') OR (one='20160304') OR (one='20181201') OR (one='20160305') OR (one='20160302') OR (one='20160303') OR (one='20181205') OR (one='20160301') OR (one='20181204') OR (one='20181203') OR (one='20181202') OR (one='20181219') OR (one='20181218') OR (one='20181217') OR (one='20140706') OR (one='20140707') OR (one='20140704') OR (one='20140705') OR (one='20140708') OR (one='20140709') OR (one='20140702') OR (one='20140703') OR (one='20140701') OR (one='20181212') OR (one='20181211') OR (one='20181210') OR (one='20181216') OR (one='20181215') OR (one='20181214') OR (one='20181213') OR (one='20101112') OR (one='20101113') OR (one='20101110') OR (one='20101111') OR (one='20101116') " +
            "OR (one='20111203') OR (one='20111204') OR (one='20111205') OR (one='20111206') OR (one='20111207') OR (one='20111208') OR (one='20111209') OR (one='20150801') OR (one='20170412') OR (one='20170411') OR (one='20170414') OR (one='20170413') OR (one='20170410') OR (one='20170419') OR (one='20170416') OR (one='20170415') OR (one='20170418') OR (one='20170417') OR (one='20150802') OR (one='20150803') OR (one='20150804') OR (one='20150805') OR (one='20150806') OR (one='20150807') OR (one='20150808') OR (one='20150809') OR (one='20170423') OR (one='20170422') OR (one='20170425') OR (one='20170424') OR (one='20170421') OR (one='20170420') OR (one='20170427') OR (one='20170426') OR (one='20170429') OR (one='20170428') OR (one='20150820') OR (one='20150821') OR (one='20150822') OR (one='20150823') OR (one='20150824') OR (one='20150825') OR (one='20150826') OR (one='20150827') OR (one='20150828') OR (one='20150829') OR (one='20150810') OR (one='20150811') OR (one='20150812') OR (one='20170401') OR (one='20170403') OR (one='20170402') OR (one='20170409') OR (one='20170408') OR (one='20170405') OR (one='20170404') OR (one='20170407') OR (one='20170406') OR (one='20150813') OR (one='20150814') OR (one='20150815') OR (one='20150816') OR (one='20150817') OR (one='20150818') OR (one='20150819') OR (one='20140630') OR (one='20190902') OR (one='20190901') OR (one='20190904') OR (one='20190903') OR (one='20190906') OR (one='20190905') OR (one='20190908') OR (one='20190907') OR (one='20190909') OR (one='20101024') OR (one='20101025') OR (one='20101022') OR (one='20101023') OR (one='20101028') OR (one='20101029') OR (one='20101026') OR (one='20101027') OR (one='20101031') OR (one='20101030') OR (one='20101013') OR (one='20101014') OR (one='20101011') OR (one='20101012') OR (one='20101017') OR (one='20101018') OR (one='20101015') OR (one='20101016') OR (one='20101019') OR (one='20101020') OR (one='20101021') OR (one='20101002') OR (one='20101003') OR (one='20101001') OR (one='20101006') OR (one='20101007') OR (one='20101004') OR (one='20101005') OR (one='20101008') OR (one='20101009') OR (one='20101010') OR (one='20160209') OR (one='20160207') OR (one='20160208') OR (one='20160205') OR (one='20160206') OR (one='20160203') OR (one='20160204') OR (one='20160201') OR (one='20160202') OR (one='20140607') OR (one='20140608') OR (one='20140605') OR (one='20140606') OR (one='20140609') OR (one='20140603') OR (one='20140604') OR (one='20140601') OR (one='20140602') OR (one='20181129') OR (one='20160229') OR (one='20140618') OR (one='20140619') OR (one='20140616') OR (one='20140617') OR (one='20140610') OR (one='20140611') OR (one='20160220') OR (one='20140614') OR (one='20181120') OR (one='20140615') OR (one='20140612') OR (one='20140613') OR (one='20160227') OR (one='20181124') OR (one='20160228') OR (one='20181123') OR (one='20160225') OR (one='20181122') OR (one='20160226') OR (one='20181121') OR (one='20160223') OR (one='20181128') OR (one='20160224') OR (one='20181127') OR (one='20160221') OR (one='20181126') OR (one='20160222') OR (one='20181125') OR (one='20160218') OR (one='20160219') OR (one='20140629') OR (one='20140627') OR (one='20140628') OR (one='20140621') OR (one='20140622') OR (one='20140620') OR (one='20140625') OR (one='20140626') OR (one='20181130') OR (one='20140623') OR (one='20140624') OR (one='20160216') OR (one='20160217') OR (one='20160214') OR (one='20160215') OR (one='20160212') OR (one='20160213') OR (one='20160210') OR (one='20160211') OR (one='20110828') OR (one='20110829') OR (one='20130430') OR (one='20110820') OR (one='20110821') OR (one='20110822') OR (one='20110823') OR (one='20110824') OR (one='20110825') OR (one='20110826') OR (one='20110827') OR (one='20110817') OR (one='20180703') OR (one='20110818') OR (one='20180702') OR (one='20110819') OR (one='20180701') OR (one='20180707') OR (one='20180706') OR (one='20180705') OR (one='20180704') OR (one='20180709') OR (one='20180708') OR (one='20110810') OR (one='20110811') OR (one='20110812') OR (one='20110813') OR (one='20110814') OR (one='20110815') OR (one='20110816') OR (one='20110806') OR (one='20110807') OR (one='20110808') OR (one='20110809') OR (one='20141106') OR (one='20141105') OR (one='20141104') OR (one='20141103') OR (one='20141109') OR (one='20141108') OR (one='20141107') OR (one='20110801') OR (one='20110802') OR (one='20141102') OR (one='20110803') OR (one='20141101') OR (one='20110804') OR (one='20110805') OR (one='20141117') OR (one='20141116') OR (one='20141115') OR (one='20141114') OR (one='20141119') OR (one='20141118') OR (one='20141113') OR (one='20141112') OR (one='20141111') OR (one='20141110') OR (one='20080208') OR (one='20080209') OR (one='20080206') OR (one='20080207') OR (one='20080204') OR (one='20080205') OR (one='20080202') OR (one='20080203') OR (one='20080201') OR (one='20130404') OR (one='20130405') OR (one='20130406') OR (one='20130407') OR (one='20130401') OR (one='20130402') OR (one='20130403') OR (one='20080219') OR (one='20080217') OR (one='20080218') OR (one='20080215') OR (one='20080216') OR (one='20080213') OR (one='20080214') OR (one='20190930') OR (one='20080211') OR (one='20080212') OR (one='20080210') OR (one='20130408') OR (one='20130409') OR (one='20130415') OR (one='20130416') OR (one='20130417') OR (one='20130418') OR (one='20130411') OR (one='20130412') OR (one='20130413') OR (one='20130414') OR (one='20130410') OR (one='20080228') OR (one='20080229') OR (one='20080226') OR (one='20080227') OR (one='20080224') OR (one='20190920') OR (one='20080225') OR (one='20080222') OR (one='20190922') OR (one='20080223') OR (one='20190921') OR (one='20080220') OR (one='20190924') OR (one='20080221') OR (one='20190923') OR (one='20190926') OR (one='20190925') OR (one='20190928') OR (one='20190927') OR (one='20190929') OR (one='20130419') OR (one='20130426') OR (one='20130427') OR (one='20130428') OR (one='20130429') OR (one='20130422') OR (one='20130423') OR (one='20130424') OR (one='20130425') OR (one='20130420') OR (one='20130421') OR (one='20190911') OR (one='20190910') OR (one='20190913') OR (one='20190912') OR (one='20190915') OR (one='20110830') OR (one='20190914') OR (one='20110831') OR (one='20190917') OR (one='20190916') OR (one='20190919') OR (one='20190918') OR (one='20100621') OR (one='20100622') OR (one='20100620') OR (one='20100625') OR (one='20100626') OR (one='20100623') OR (one='20100624') OR (one='20100629') OR (one='20190102') OR (one='20190101') OR (one='20100627') OR (one='20190104') OR (one='20100628') OR (one='20190103') OR (one='20190106') OR (one='20190105') OR (one='20120229') OR (one='20190108') OR (one='20190107') OR (one='20120227') OR (one='20120228') OR (one='20190109') OR (one='20120225') OR (one='20120226') OR (one='20120223') OR (one='20120224') OR (one='20120221') OR (one='20120222') OR (one='20120220') OR (one='20100610') OR (one='20100611') OR (one='20100614') OR (one='20100615') OR (one='20100612') OR (one='20170502') OR (one='20100613') OR (one='20170501') OR (one='20100618') OR (one='20100619') OR (one='20100616') OR (one='20100617') OR (one='20170508') OR (one='20170507') OR (one='20120218') OR (one='20120219') OR (one='20170509') OR (one='20120216') OR (one='20170504') OR (one='20120217') OR (one='20170503') OR (one='20120214') OR (one='20170506') OR (one='20120215') OR (one='20170505') OR (one='20120212') OR (one='20120213') OR (one='20120210') OR (one='20120211') OR (one='20100603') OR (one='20100604') OR (one='20100601') OR (one='20100602') OR (one='20100607') OR (one='20100608') OR (one='20100605') OR (one='20100606') OR (one='20120209') OR (one='20150909') OR (one='20120207') OR (one='20100609') OR (one='20120208') OR (one='20120205') OR (one='20120206') OR (one='20120203') OR (one='20120204') OR (one='20120201') OR (one='20150901') OR (one='20120202') OR (one='20150902') OR (one='20150903') OR (one='20150904') OR (one='20150905') OR (one='20150906') OR (one='20150907') OR (one='20150908') OR (one='20070801') OR (one='20090410') OR (one='20070802') OR (one='20090411') OR (one='20070803') OR (one='20070804') OR (one='20070805') OR (one='20070806') OR (one='20070807') OR (one='20070808') OR (one='20070809') OR (one='20141128') OR (one='20141127') OR (one='20141126') OR (one='20141125') OR (one='20141129') OR (one='20141120') OR (one='20090416') OR (one='20090417') OR (one='20090418') OR (one='20090419') OR (one='20141124') OR (one='20090412') OR (one='20141123') OR (one='20090413') OR (one='20180731') OR (one='20141122') OR (one='20090414') OR (one='20180730') OR (one='20141121') OR (one='20090415') OR (one='20070810') OR (one='20070811') OR (one='20070812') OR (one='20070813') OR (one='20070814') OR (one='20070815') OR (one='20070816') OR (one='20070817') OR (one='20070818') OR (one='20070819') OR (one='20090409') OR (one='20090405') OR (one='20141130') OR (one='20090406') OR (one='20090407') OR (one='20090408') OR (one='20090401') OR (one='20090402') OR (one='20090403') OR (one='20090404') OR (one='20070821') OR (one='20090430') OR (one='20180714') OR (one='20070822') OR (one='20180713') OR (one='20070823') OR (one='20180712') OR (one='20070824') OR (one='20180711') OR (one='20070825') OR (one='20180718') OR (one='20070826') OR (one='20180717') OR (one='20070827') OR (one='20180716') OR (one='20070828') OR (one='20180715') OR (one='20070829') OR (one='20180719') OR (one='20180710') OR (one='20070820') OR (one='20180725') OR (one='20090420') OR (one='20180724') OR (one='20090421') OR (one='20180723') OR (one='20090422') OR (one='20180722') OR (one='20180729') OR (one='20180728') OR (one='20100630') OR (one='20180727') OR (one='20180726') OR (one='20090427') OR (one='20090428') OR (one='20090429') OR (one='20090423') OR (one='20180721') OR (one='20090424') OR (one='20180720') OR (one='20070830') OR (one='20090425') OR (one='20070831') OR (one='20090426') OR (one='20181229') OR (one='20181228') OR (one='20181223') OR (one='20181222') OR (one='20181221') OR (one='20181220') OR (one='20181227') OR (one='20181226') OR (one='20181225') OR (one='20181224') OR (one='20181230') OR (one='20181231') OR (one='20181209') OR (one='20181208') OR (one='20181207') OR (one='20181206') OR (one='20160308') OR (one='20160309') OR (one='20160306') OR (one='20160307') OR (one='20160304') OR (one='20181201') OR (one='20160305') OR (one='20160302') OR (one='20160303') OR (one='20181205') OR (one='20160301') OR (one='20181204') OR (one='20181203') OR (one='20181202') OR (one='20181219') OR (one='20181218') OR (one='20181217') OR (one='20140706') OR (one='20140707') OR (one='20140704') OR (one='20140705') OR (one='20140708') OR (one='20140709') OR (one='20140702') OR (one='20140703') OR (one='20140701') OR (one='20181212') OR (one='20181211') OR (one='20181210') OR (one='20181216') OR (one='20181215') OR (one='20181214') OR (one='20181213') OR (one='20101112') OR (one='20101113') OR (one='20101110') OR (one='20101111') OR (one='20101116') " +
            "OR (one='20111203') OR (one='20111204') OR (one='20111205') OR (one='20111206') OR (one='20111207') OR (one='20111208') OR (one='20111209') OR (one='20150801') OR (one='20170412') OR (one='20170411') OR (one='20170414') OR (one='20170413') OR (one='20170410') OR (one='20170419') OR (one='20170416') OR (one='20170415') OR (one='20170418') OR (one='20170417') OR (one='20150802') OR (one='20150803') OR (one='20150804') OR (one='20150805') OR (one='20150806') OR (one='20150807') OR (one='20150808') OR (one='20150809') OR (one='20170423') OR (one='20170422') OR (one='20170425') OR (one='20170424') OR (one='20170421') OR (one='20170420') OR (one='20170427') OR (one='20170426') OR (one='20170429') OR (one='20170428') OR (one='20150820') OR (one='20150821') OR (one='20150822') OR (one='20150823') OR (one='20150824') OR (one='20150825') OR (one='20150826') OR (one='20150827') OR (one='20150828') OR (one='20150829') OR (one='20150810') OR (one='20150811') OR (one='20150812') OR (one='20170401') OR (one='20170403') OR (one='20170402') OR (one='20170409') OR (one='20170408') OR (one='20170405') OR (one='20170404') OR (one='20170407') OR (one='20170406') OR (one='20150813') OR (one='20150814') OR (one='20150815') OR (one='20150816') OR (one='20150817') OR (one='20150818') OR (one='20150819') OR (one='20140630') OR (one='20190902') OR (one='20190901') OR (one='20190904') OR (one='20190903') OR (one='20190906') OR (one='20190905') OR (one='20190908') OR (one='20190907') OR (one='20190909') OR (one='20101024') OR (one='20101025') OR (one='20101022') OR (one='20101023') OR (one='20101028') OR (one='20101029') OR (one='20101026') OR (one='20101027') OR (one='20101031') OR (one='20101030') OR (one='20101013') OR (one='20101014') OR (one='20101011') OR (one='20101012') OR (one='20101017') OR (one='20101018') OR (one='20101015') OR (one='20101016') OR (one='20101019') OR (one='20101020') OR (one='20101021') OR (one='20101002') OR (one='20101003') OR (one='20101001') OR (one='20101006') OR (one='20101007') OR (one='20101004') OR (one='20101005') OR (one='20101008') OR (one='20101009') OR (one='20101010') OR (one='20160209') OR (one='20160207') OR (one='20160208') OR (one='20160205') OR (one='20160206') OR (one='20160203') OR (one='20160204') OR (one='20160201') OR (one='20160202') OR (one='20140607') OR (one='20140608') OR (one='20140605') OR (one='20140606') OR (one='20140609') OR (one='20140603') OR (one='20140604') OR (one='20140601') OR (one='20140602') OR (one='20181129') OR (one='20160229') OR (one='20140618') OR (one='20140619') OR (one='20140616') OR (one='20140617') OR (one='20140610') OR (one='20140611') OR (one='20160220') OR (one='20140614') OR (one='20181120') OR (one='20140615') OR (one='20140612') OR (one='20140613') OR (one='20160227') OR (one='20181124') OR (one='20160228') OR (one='20181123') OR (one='20160225') OR (one='20181122') OR (one='20160226') OR (one='20181121') OR (one='20160223') OR (one='20181128') OR (one='20160224') OR (one='20181127') OR (one='20160221') OR (one='20181126') OR (one='20160222') OR (one='20181125') OR (one='20160218') OR (one='20160219') OR (one='20140629') OR (one='20140627') OR (one='20140628') OR (one='20140621') OR (one='20140622') OR (one='20140620') OR (one='20140625') OR (one='20140626') OR (one='20181130') OR (one='20140623') OR (one='20140624') OR (one='20160216') OR (one='20160217') OR (one='20160214') OR (one='20160215') OR (one='20160212') OR (one='20160213') OR (one='20160210') OR (one='20160211') OR (one='20110828') OR (one='20110829') OR (one='20130430') OR (one='20110820') OR (one='20110821') OR (one='20110822') OR (one='20110823') OR (one='20110824') OR (one='20110825') OR (one='20110826') OR (one='20110827') OR (one='20110817') OR (one='20180703') OR (one='20110818') OR (one='20180702') OR (one='20110819') OR (one='20180701') OR (one='20180707') OR (one='20180706') OR (one='20180705') OR (one='20180704') OR (one='20180709') OR (one='20180708') OR (one='20110810') OR (one='20110811') OR (one='20110812') OR (one='20110813') OR (one='20110814') OR (one='20110815') OR (one='20110816') OR (one='20110806') OR (one='20110807') OR (one='20110808') OR (one='20110809') OR (one='20141106') OR (one='20141105') OR (one='20141104') OR (one='20141103') OR (one='20141109') OR (one='20141108') OR (one='20141107') OR (one='20110801') OR (one='20110802') OR (one='20141102') OR (one='20110803') OR (one='20141101') OR (one='20110804') OR (one='20110805') OR (one='20141117') OR (one='20141116') OR (one='20141115') OR (one='20141114') OR (one='20141119') OR (one='20141118') OR (one='20141113') OR (one='20141112') OR (one='20141111') OR (one='20141110') OR (one='20080208') OR (one='20080209') OR (one='20080206') OR (one='20080207') OR (one='20080204') OR (one='20080205') OR (one='20080202') OR (one='20080203') OR (one='20080201') OR (one='20130404') OR (one='20130405') OR (one='20130406') OR (one='20130407') OR (one='20130401') OR (one='20130402') OR (one='20130403') OR (one='20080219') OR (one='20080217') OR (one='20080218') OR (one='20080215') OR (one='20080216') OR (one='20080213') OR (one='20080214') OR (one='20190930') OR (one='20080211') OR (one='20080212') OR (one='20080210') OR (one='20130408') OR (one='20130409') OR (one='20130415') OR (one='20130416') OR (one='20130417') OR (one='20130418') OR (one='20130411') OR (one='20130412') OR (one='20130413') OR (one='20130414') OR (one='20130410') OR (one='20080228') OR (one='20080229') OR (one='20080226') OR (one='20080227') OR (one='20080224') OR (one='20190920') OR (one='20080225') OR (one='20080222') OR (one='20190922') OR (one='20080223') OR (one='20190921') OR (one='20080220') OR (one='20190924') OR (one='20080221') OR (one='20190923') OR (one='20190926') OR (one='20190925') OR (one='20190928') OR (one='20190927') OR (one='20190929') OR (one='20130419') OR (one='20130426') OR (one='20130427') OR (one='20130428') OR (one='20130429') OR (one='20130422') OR (one='20130423') OR (one='20130424') OR (one='20130425') OR (one='20130420') OR (one='20130421') OR (one='20190911') OR (one='20190910') OR (one='20190913') OR (one='20190912') OR (one='20190915') OR (one='20110830') OR (one='20190914') OR (one='20110831') OR (one='20190917') OR (one='20190916') OR (one='20190919') OR (one='20190918') OR (one='20100621') OR (one='20100622') OR (one='20100620') OR (one='20100625') OR (one='20100626') OR (one='20100623') OR (one='20100624') OR (one='20100629') OR (one='20190102') OR (one='20190101') OR (one='20100627') OR (one='20190104') OR (one='20100628') OR (one='20190103') OR (one='20190106') OR (one='20190105') OR (one='20120229') OR (one='20190108') OR (one='20190107') OR (one='20120227') OR (one='20120228') OR (one='20190109') OR (one='20120225') OR (one='20120226') OR (one='20120223') OR (one='20120224') OR (one='20120221') OR (one='20120222') OR (one='20120220') OR (one='20100610') OR (one='20100611') OR (one='20100614') OR (one='20100615') OR (one='20100612') OR (one='20170502') OR (one='20100613') OR (one='20170501') OR (one='20100618') OR (one='20100619') OR (one='20100616') OR (one='20100617') OR (one='20170508') OR (one='20170507') OR (one='20120218') OR (one='20120219') OR (one='20170509') OR (one='20120216') OR (one='20170504') OR (one='20120217') OR (one='20170503') OR (one='20120214') OR (one='20170506') OR (one='20120215') OR (one='20170505') OR (one='20120212') OR (one='20120213') OR (one='20120210') OR (one='20120211') OR (one='20100603') OR (one='20100604') OR (one='20100601') OR (one='20100602') OR (one='20100607') OR (one='20100608') OR (one='20100605') OR (one='20100606') OR (one='20120209') OR (one='20150909') OR (one='20120207') OR (one='20100609') OR (one='20120208') OR (one='20120205') OR (one='20120206') OR (one='20120203') OR (one='20120204') OR (one='20120201') OR (one='20150901') OR (one='20120202') OR (one='20150902') OR (one='20150903') OR (one='20150904') OR (one='20150905') OR (one='20150906') OR (one='20150907') OR (one='20150908') OR (one='20070801') OR (one='20090410') OR (one='20070802') OR (one='20090411') OR (one='20070803') OR (one='20070804') OR (one='20070805') OR (one='20070806') OR (one='20070807') OR (one='20070808') OR (one='20070809') OR (one='20141128') OR (one='20141127') OR (one='20141126') OR (one='20141125') OR (one='20141129') OR (one='20141120') OR (one='20090416') OR (one='20090417') OR (one='20090418') OR (one='20090419') OR (one='20141124') OR (one='20090412') OR (one='20141123') OR (one='20090413') OR (one='20180731') OR (one='20141122') OR (one='20090414') OR (one='20180730') OR (one='20141121') OR (one='20090415') OR (one='20070810') OR (one='20070811') OR (one='20070812') OR (one='20070813') OR (one='20070814') OR (one='20070815') OR (one='20070816') OR (one='20070817') OR (one='20070818') OR (one='20070819') OR (one='20090409') OR (one='20090405') OR (one='20141130') OR (one='20090406') OR (one='20090407') OR (one='20090408') OR (one='20090401') OR (one='20090402') OR (one='20090403') OR (one='20090404') OR (one='20070821') OR (one='20090430') OR (one='20180714') OR (one='20070822') OR (one='20180713') OR (one='20070823') OR (one='20180712') OR (one='20070824') OR (one='20180711') OR (one='20070825') OR (one='20180718') OR (one='20070826') OR (one='20180717') OR (one='20070827') OR (one='20180716') OR (one='20070828') OR (one='20180715') OR (one='20070829') OR (one='20180719') OR (one='20180710') OR (one='20070820') OR (one='20180725') OR (one='20090420') OR (one='20180724') OR (one='20090421') OR (one='20180723') OR (one='20090422') OR (one='20180722') OR (one='20180729') OR (one='20180728') OR (one='20100630') OR (one='20180727') OR (one='20180726') OR (one='20090427') OR (one='20090428') OR (one='20090429') OR (one='20090423') OR (one='20180721') OR (one='20090424') OR (one='20180720') OR (one='20070830') OR (one='20090425') OR (one='20070831') OR (one='20090426') OR (one='20181229') OR (one='20181228') OR (one='20181223') OR (one='20181222') OR (one='20181221') OR (one='20181220') OR (one='20181227') OR (one='20181226') OR (one='20181225') OR (one='20181224') OR (one='20181230') OR (one='20181231') OR (one='20181209') OR (one='20181208') OR (one='20181207') OR (one='20181206') OR (one='20160308') OR (one='20160309') OR (one='20160306') OR (one='20160307') OR (one='20160304') OR (one='20181201') OR (one='20160305') OR (one='20160302') OR (one='20160303') OR (one='20181205') OR (one='20160301') OR (one='20181204') OR (one='20181203') OR (one='20181202') OR (one='20181219') OR (one='20181218') OR (one='20181217') OR (one='20140706') OR (one='20140707') OR (one='20140704') OR (one='20140705') OR (one='20140708') OR (one='20140709') OR (one='20140702') OR (one='20140703') OR (one='20140701') OR (one='20181212') OR (one='20181211') OR (one='20181210') OR (one='20181216') OR (one='20181215') OR (one='20181214') OR (one='20181213') OR (one='20101112') OR (one='20101113') OR (one='20101110') OR (one='20101111') OR (one='20101116') " +
            "OR (one='20111203') OR (one='20111204') OR (one='20111205') OR (one='20111206') OR (one='20111207') OR (one='20111208') OR (one='20111209') OR (one='20150801') OR (one='20170412') OR (one='20170411') OR (one='20170414') OR (one='20170413') OR (one='20170410') OR (one='20170419') OR (one='20170416') OR (one='20170415') OR (one='20170418') OR (one='20170417') OR (one='20150802') OR (one='20150803') OR (one='20150804') OR (one='20150805') OR (one='20150806') OR (one='20150807') OR (one='20150808') OR (one='20150809') OR (one='20170423') OR (one='20170422') OR (one='20170425') OR (one='20170424') OR (one='20170421') OR (one='20170420') OR (one='20170427') OR (one='20170426') OR (one='20170429') OR (one='20170428') OR (one='20150820') OR (one='20150821') OR (one='20150822') OR (one='20150823') OR (one='20150824') OR (one='20150825') OR (one='20150826') OR (one='20150827') OR (one='20150828') OR (one='20150829') OR (one='20150810') OR (one='20150811') OR (one='20150812') OR (one='20170401') OR (one='20170403') OR (one='20170402') OR (one='20170409') OR (one='20170408') OR (one='20170405') OR (one='20170404') OR (one='20170407') OR (one='20170406') OR (one='20150813') OR (one='20150814') OR (one='20150815') OR (one='20150816') OR (one='20150817') OR (one='20150818') OR (one='20150819') OR (one='20140630') OR (one='20190902') OR (one='20190901') OR (one='20190904') OR (one='20190903') OR (one='20190906') OR (one='20190905') OR (one='20190908') OR (one='20190907') OR (one='20190909') OR (one='20101024') OR (one='20101025') OR (one='20101022') OR (one='20101023') OR (one='20101028') OR (one='20101029') OR (one='20101026') OR (one='20101027') OR (one='20101031') OR (one='20101030') OR (one='20101013') OR (one='20101014') OR (one='20101011') OR (one='20101012') OR (one='20101017') OR (one='20101018') OR (one='20101015') OR (one='20101016') OR (one='20101019') OR (one='20101020') OR (one='20101021') OR (one='20101002') OR (one='20101003') OR (one='20101001') OR (one='20101006') OR (one='20101007') OR (one='20101004') OR (one='20101005') OR (one='20101008') OR (one='20101009') OR (one='20101010') OR (one='20160209') OR (one='20160207') OR (one='20160208') OR (one='20160205') OR (one='20160206') OR (one='20160203') OR (one='20160204') OR (one='20160201') OR (one='20160202') OR (one='20140607') OR (one='20140608') OR (one='20140605') OR (one='20140606') OR (one='20140609') OR (one='20140603') OR (one='20140604') OR (one='20140601') OR (one='20140602') OR (one='20181129') OR (one='20160229') OR (one='20140618') OR (one='20140619') OR (one='20140616') OR (one='20140617') OR (one='20140610') OR (one='20140611') OR (one='20160220') OR (one='20140614') OR (one='20181120') OR (one='20140615') OR (one='20140612') OR (one='20140613') OR (one='20160227') OR (one='20181124') OR (one='20160228') OR (one='20181123') OR (one='20160225') OR (one='20181122') OR (one='20160226') OR (one='20181121') OR (one='20160223') OR (one='20181128') OR (one='20160224') OR (one='20181127') OR (one='20160221') OR (one='20181126') OR (one='20160222') OR (one='20181125') OR (one='20160218') OR (one='20160219') OR (one='20140629') OR (one='20140627') OR (one='20140628') OR (one='20140621') OR (one='20140622') OR (one='20140620') OR (one='20140625') OR (one='20140626') OR (one='20181130') OR (one='20140623') OR (one='20140624') OR (one='20160216') OR (one='20160217') OR (one='20160214') OR (one='20160215') OR (one='20160212') OR (one='20160213') OR (one='20160210') OR (one='20160211') OR (one='20110828') OR (one='20110829') OR (one='20130430') OR (one='20110820') OR (one='20110821') OR (one='20110822') OR (one='20110823') OR (one='20110824') OR (one='20110825') OR (one='20110826') OR (one='20110827') OR (one='20110817') OR (one='20180703') OR (one='20110818') OR (one='20180702') OR (one='20110819') OR (one='20180701') OR (one='20180707') OR (one='20180706') OR (one='20180705') OR (one='20180704') OR (one='20180709') OR (one='20180708') OR (one='20110810') OR (one='20110811') OR (one='20110812') OR (one='20110813') OR (one='20110814') OR (one='20110815') OR (one='20110816') OR (one='20110806') OR (one='20110807') OR (one='20110808') OR (one='20110809') OR (one='20141106') OR (one='20141105') OR (one='20141104') OR (one='20141103') OR (one='20141109') OR (one='20141108') OR (one='20141107') OR (one='20110801') OR (one='20110802') OR (one='20141102') OR (one='20110803') OR (one='20141101') OR (one='20110804') OR (one='20110805') OR (one='20141117') OR (one='20141116') OR (one='20141115') OR (one='20141114') OR (one='20141119') OR (one='20141118') OR (one='20141113') OR (one='20141112') OR (one='20141111') OR (one='20141110') OR (one='20080208') OR (one='20080209') OR (one='20080206') OR (one='20080207') OR (one='20080204') OR (one='20080205') OR (one='20080202') OR (one='20080203') OR (one='20080201') OR (one='20130404') OR (one='20130405') OR (one='20130406') OR (one='20130407') OR (one='20130401') OR (one='20130402') OR (one='20130403') OR (one='20080219') OR (one='20080217') OR (one='20080218') OR (one='20080215') OR (one='20080216') OR (one='20080213') OR (one='20080214') OR (one='20190930') OR (one='20080211') OR (one='20080212') OR (one='20080210') OR (one='20130408') OR (one='20130409') OR (one='20130415') OR (one='20130416') OR (one='20130417') OR (one='20130418') OR (one='20130411') OR (one='20130412') OR (one='20130413') OR (one='20130414') OR (one='20130410') OR (one='20080228') OR (one='20080229') OR (one='20080226') OR (one='20080227') OR (one='20080224') OR (one='20190920') OR (one='20080225') OR (one='20080222') OR (one='20190922') OR (one='20080223') OR (one='20190921') OR (one='20080220') OR (one='20190924') OR (one='20080221') OR (one='20190923') OR (one='20190926') OR (one='20190925') OR (one='20190928') OR (one='20190927') OR (one='20190929') OR (one='20130419') OR (one='20130426') OR (one='20130427') OR (one='20130428') OR (one='20130429') OR (one='20130422') OR (one='20130423') OR (one='20130424') OR (one='20130425') OR (one='20130420') OR (one='20130421') OR (one='20190911') OR (one='20190910') OR (one='20190913') OR (one='20190912') OR (one='20190915') OR (one='20110830') OR (one='20190914') OR (one='20110831') OR (one='20190917') OR (one='20190916') OR (one='20190919') OR (one='20190918') OR (one='20100621') OR (one='20100622') OR (one='20100620') OR (one='20100625') OR (one='20100626') OR (one='20100623') OR (one='20100624') OR (one='20100629') OR (one='20190102') OR (one='20190101') OR (one='20100627') OR (one='20190104') OR (one='20100628') OR (one='20190103') OR (one='20190106') OR (one='20190105') OR (one='20120229') OR (one='20190108') OR (one='20190107') OR (one='20120227') OR (one='20120228') OR (one='20190109') OR (one='20120225') OR (one='20120226') OR (one='20120223') OR (one='20120224') OR (one='20120221') OR (one='20120222') OR (one='20120220') OR (one='20100610') OR (one='20100611') OR (one='20100614') OR (one='20100615') OR (one='20100612') OR (one='20170502') OR (one='20100613') OR (one='20170501') OR (one='20100618') OR (one='20100619') OR (one='20100616') OR (one='20100617') OR (one='20170508') OR (one='20170507') OR (one='20120218') OR (one='20120219') OR (one='20170509') OR (one='20120216') OR (one='20170504') OR (one='20120217') OR (one='20170503') OR (one='20120214') OR (one='20170506') OR (one='20120215') OR (one='20170505') OR (one='20120212') OR (one='20120213') OR (one='20120210') OR (one='20120211') OR (one='20100603') OR (one='20100604') OR (one='20100601') OR (one='20100602') OR (one='20100607') OR (one='20100608') OR (one='20100605') OR (one='20100606') OR (one='20120209') OR (one='20150909') OR (one='20120207') OR (one='20100609') OR (one='20120208') OR (one='20120205') OR (one='20120206') OR (one='20120203') OR (one='20120204') OR (one='20120201') OR (one='20150901') OR (one='20120202') OR (one='20150902') OR (one='20150903') OR (one='20150904') OR (one='20150905') OR (one='20150906') OR (one='20150907') OR (one='20150908') OR (one='20070801') OR (one='20090410') OR (one='20070802') OR (one='20090411') OR (one='20070803') OR (one='20070804') OR (one='20070805') OR (one='20070806') OR (one='20070807') OR (one='20070808') OR (one='20070809') OR (one='20141128') OR (one='20141127') OR (one='20141126') OR (one='20141125') OR (one='20141129') OR (one='20141120') OR (one='20090416') OR (one='20090417') OR (one='20090418') OR (one='20090419') OR (one='20141124') OR (one='20090412') OR (one='20141123') OR (one='20090413') OR (one='20180731') OR (one='20141122') OR (one='20090414') OR (one='20180730') OR (one='20141121') OR (one='20090415') OR (one='20070810') OR (one='20070811') OR (one='20070812') OR (one='20070813') OR (one='20070814') OR (one='20070815') OR (one='20070816') OR (one='20070817') OR (one='20070818') OR (one='20070819') OR (one='20090409') OR (one='20090405') OR (one='20141130') OR (one='20090406') OR (one='20090407') OR (one='20090408') OR (one='20090401') OR (one='20090402') OR (one='20090403') OR (one='20090404') OR (one='20070821') OR (one='20090430') OR (one='20180714') OR (one='20070822') OR (one='20180713') OR (one='20070823') OR (one='20180712') OR (one='20070824') OR (one='20180711') OR (one='20070825') OR (one='20180718') OR (one='20070826') OR (one='20180717') OR (one='20070827') OR (one='20180716') OR (one='20070828') OR (one='20180715') OR (one='20070829') OR (one='20180719') OR (one='20180710') OR (one='20070820') OR (one='20180725') OR (one='20090420') OR (one='20180724') OR (one='20090421') OR (one='20180723') OR (one='20090422') OR (one='20180722') OR (one='20180729') OR (one='20180728') OR (one='20100630') OR (one='20180727') OR (one='20180726') OR (one='20090427') OR (one='20090428') OR (one='20090429') OR (one='20090423') OR (one='20180721') OR (one='20090424') OR (one='20180720') OR (one='20070830') OR (one='20090425') OR (one='20070831') OR (one='20090426') OR (one='20181229') OR (one='20181228') OR (one='20181223') OR (one='20181222') OR (one='20181221') OR (one='20181220') OR (one='20181227') OR (one='20181226') OR (one='20181225') OR (one='20181224') OR (one='20181230') OR (one='20181231') OR (one='20181209') OR (one='20181208') OR (one='20181207') OR (one='20181206') OR (one='20160308') OR (one='20160309') OR (one='20160306') OR (one='20160307') OR (one='20160304') OR (one='20181201') OR (one='20160305') OR (one='20160302') OR (one='20160303') OR (one='20181205') OR (one='20160301') OR (one='20181204') OR (one='20181203') OR (one='20181202') OR (one='20181219') OR (one='20181218') OR (one='20181217') OR (one='20140706') OR (one='20140707') OR (one='20140704') OR (one='20140705') OR (one='20140708') OR (one='20140709') OR (one='20140702') OR (one='20140703') OR (one='20140701') OR (one='20181212') OR (one='20181211') OR (one='20181210') OR (one='20181216') OR (one='20181215') OR (one='20181214') OR (one='20181213') OR (one='20101112') OR (one='20101113') OR (one='20101110') OR (one='20101111') OR (one='20101116') " +
            "OR (one='20111203') OR (one='20111204') OR (one='20111205') OR (one='20111206') OR (one='20111207') OR (one='20111208') OR (one='20111209') OR (one='20150801') OR (one='20170412') OR (one='20170411') OR (one='20170414') OR (one='20170413') OR (one='20170410') OR (one='20170419') OR (one='20170416') OR (one='20170415') OR (one='20170418') OR (one='20170417') OR (one='20150802') OR (one='20150803') OR (one='20150804') OR (one='20150805') OR (one='20150806') OR (one='20150807') OR (one='20150808') OR (one='20150809') OR (one='20170423') OR (one='20170422') OR (one='20170425') OR (one='20170424') OR (one='20170421') OR (one='20170420') OR (one='20170427') OR (one='20170426') OR (one='20170429') OR (one='20170428') OR (one='20150820') OR (one='20150821') OR (one='20150822') OR (one='20150823') OR (one='20150824') OR (one='20150825') OR (one='20150826') OR (one='20150827') OR (one='20150828') OR (one='20150829') OR (one='20150810') OR (one='20150811') OR (one='20150812') OR (one='20170401') OR (one='20170403') OR (one='20170402') OR (one='20170409') OR (one='20170408') OR (one='20170405') OR (one='20170404') OR (one='20170407') OR (one='20170406') OR (one='20150813') OR (one='20150814') OR (one='20150815') OR (one='20150816') OR (one='20150817') OR (one='20150818') OR (one='20150819') OR (one='20140630') OR (one='20190902') OR (one='20190901') OR (one='20190904') OR (one='20190903') OR (one='20190906') OR (one='20190905') OR (one='20190908') OR (one='20190907') OR (one='20190909') OR (one='20101024') OR (one='20101025') OR (one='20101022') OR (one='20101023') OR (one='20101028') OR (one='20101029') OR (one='20101026') OR (one='20101027') OR (one='20101031') OR (one='20101030') OR (one='20101013') OR (one='20101014') OR (one='20101011') OR (one='20101012') OR (one='20101017') OR (one='20101018') OR (one='20101015') OR (one='20101016') OR (one='20101019') OR (one='20101020') OR (one='20101021') OR (one='20101002') OR (one='20101003') OR (one='20101001') OR (one='20101006') OR (one='20101007') OR (one='20101004') OR (one='20101005') OR (one='20101008') OR (one='20101009') OR (one='20101010') OR (one='20160209') OR (one='20160207') OR (one='20160208') OR (one='20160205') OR (one='20160206') OR (one='20160203') OR (one='20160204') OR (one='20160201') OR (one='20160202') OR (one='20140607') OR (one='20140608') OR (one='20140605') OR (one='20140606') OR (one='20140609') OR (one='20140603') OR (one='20140604') OR (one='20140601') OR (one='20140602') OR (one='20181129') OR (one='20160229') OR (one='20140618') OR (one='20140619') OR (one='20140616') OR (one='20140617') OR (one='20140610') OR (one='20140611') OR (one='20160220') OR (one='20140614') OR (one='20181120') OR (one='20140615') OR (one='20140612') OR (one='20140613') OR (one='20160227') OR (one='20181124') OR (one='20160228') OR (one='20181123') OR (one='20160225') OR (one='20181122') OR (one='20160226') OR (one='20181121') OR (one='20160223') OR (one='20181128') OR (one='20160224') OR (one='20181127') OR (one='20160221') OR (one='20181126') OR (one='20160222') OR (one='20181125') OR (one='20160218') OR (one='20160219') OR (one='20140629') OR (one='20140627') OR (one='20140628') OR (one='20140621') OR (one='20140622') OR (one='20140620') OR (one='20140625') OR (one='20140626') OR (one='20181130') OR (one='20140623') OR (one='20140624') OR (one='20160216') OR (one='20160217') OR (one='20160214') OR (one='20160215') OR (one='20160212') OR (one='20160213') OR (one='20160210') OR (one='20160211') OR (one='20110828') OR (one='20110829') OR (one='20130430') OR (one='20110820') OR (one='20110821') OR (one='20110822') OR (one='20110823') OR (one='20110824') OR (one='20110825') OR (one='20110826') OR (one='20110827') OR (one='20110817') OR (one='20180703') OR (one='20110818') OR (one='20180702') OR (one='20110819') OR (one='20180701') OR (one='20180707') OR (one='20180706') OR (one='20180705') OR (one='20180704') OR (one='20180709') OR (one='20180708') OR (one='20110810') OR (one='20110811') OR (one='20110812') OR (one='20110813') OR (one='20110814') OR (one='20110815') OR (one='20110816') OR (one='20110806') OR (one='20110807') OR (one='20110808') OR (one='20110809') OR (one='20141106') OR (one='20141105') OR (one='20141104') OR (one='20141103') OR (one='20141109') OR (one='20141108') OR (one='20141107') OR (one='20110801') OR (one='20110802') OR (one='20141102') OR (one='20110803') OR (one='20141101') OR (one='20110804') OR (one='20110805') OR (one='20141117') OR (one='20141116') OR (one='20141115') OR (one='20141114') OR (one='20141119') OR (one='20141118') OR (one='20141113') OR (one='20141112') OR (one='20141111') OR (one='20141110') OR (one='20080208') OR (one='20080209') OR (one='20080206') OR (one='20080207') OR (one='20080204') OR (one='20080205') OR (one='20080202') OR (one='20080203') OR (one='20080201') OR (one='20130404') OR (one='20130405') OR (one='20130406') OR (one='20130407') OR (one='20130401') OR (one='20130402') OR (one='20130403') OR (one='20080219') OR (one='20080217') OR (one='20080218') OR (one='20080215') OR (one='20080216') OR (one='20080213') OR (one='20080214') OR (one='20190930') OR (one='20080211') OR (one='20080212') OR (one='20080210') OR (one='20130408') OR (one='20130409') OR (one='20130415') OR (one='20130416') OR (one='20130417') OR (one='20130418') OR (one='20130411') OR (one='20130412') OR (one='20130413') OR (one='20130414') OR (one='20130410') OR (one='20080228') OR (one='20080229') OR (one='20080226') OR (one='20080227') OR (one='20080224') OR (one='20190920') OR (one='20080225') OR (one='20080222') OR (one='20190922') OR (one='20080223') OR (one='20190921') OR (one='20080220') OR (one='20190924') OR (one='20080221') OR (one='20190923') OR (one='20190926') OR (one='20190925') OR (one='20190928') OR (one='20190927') OR (one='20190929') OR (one='20130419') OR (one='20130426') OR (one='20130427') OR (one='20130428') OR (one='20130429') OR (one='20130422') OR (one='20130423') OR (one='20130424') OR (one='20130425') OR (one='20130420') OR (one='20130421') OR (one='20190911') OR (one='20190910') OR (one='20190913') OR (one='20190912') OR (one='20190915') OR (one='20110830') OR (one='20190914') OR (one='20110831') OR (one='20190917') OR (one='20190916') OR (one='20190919') OR (one='20190918') OR (one='20100621') OR (one='20100622') OR (one='20100620') OR (one='20100625') OR (one='20100626') OR (one='20100623') OR (one='20100624') OR (one='20100629') OR (one='20190102') OR (one='20190101') OR (one='20100627') OR (one='20190104') OR (one='20100628') OR (one='20190103') OR (one='20190106') OR (one='20190105') OR (one='20120229') OR (one='20190108') OR (one='20190107') OR (one='20120227') OR (one='20120228') OR (one='20190109') OR (one='20120225') OR (one='20120226') OR (one='20120223') OR (one='20120224') OR (one='20120221') OR (one='20120222') OR (one='20120220') OR (one='20100610') OR (one='20100611') OR (one='20100614') OR (one='20100615') OR (one='20100612') OR (one='20170502') OR (one='20100613') OR (one='20170501') OR (one='20100618') OR (one='20100619') OR (one='20100616') OR (one='20100617') OR (one='20170508') OR (one='20170507') OR (one='20120218') OR (one='20120219') OR (one='20170509') OR (one='20120216') OR (one='20170504') OR (one='20120217') OR (one='20170503') OR (one='20120214') OR (one='20170506') OR (one='20120215') OR (one='20170505') OR (one='20120212') OR (one='20120213') OR (one='20120210') OR (one='20120211') OR (one='20100603') OR (one='20100604') OR (one='20100601') OR (one='20100602') OR (one='20100607') OR (one='20100608') OR (one='20100605') OR (one='20100606') OR (one='20120209') OR (one='20150909') OR (one='20120207') OR (one='20100609') OR (one='20120208') OR (one='20120205') OR (one='20120206') OR (one='20120203') OR (one='20120204') OR (one='20120201') OR (one='20150901') OR (one='20120202') OR (one='20150902') OR (one='20150903') OR (one='20150904') OR (one='20150905') OR (one='20150906') OR (one='20150907') OR (one='20150908') OR (one='20070801') OR (one='20090410') OR (one='20070802') OR (one='20090411') OR (one='20070803') OR (one='20070804') OR (one='20070805') OR (one='20070806') OR (one='20070807') OR (one='20070808') OR (one='20070809') OR (one='20141128') OR (one='20141127') OR (one='20141126') OR (one='20141125') OR (one='20141129') OR (one='20141120') OR (one='20090416') OR (one='20090417') OR (one='20090418') OR (one='20090419') OR (one='20141124') OR (one='20090412') OR (one='20141123') OR (one='20090413') OR (one='20180731') OR (one='20141122') OR (one='20090414') OR (one='20180730') OR (one='20141121') OR (one='20090415') OR (one='20070810') OR (one='20070811') OR (one='20070812') OR (one='20070813') OR (one='20070814') OR (one='20070815') OR (one='20070816') OR (one='20070817') OR (one='20070818') OR (one='20070819') OR (one='20090409') OR (one='20090405') OR (one='20141130') OR (one='20090406') OR (one='20090407') OR (one='20090408') OR (one='20090401') OR (one='20090402') OR (one='20090403') OR (one='20090404') OR (one='20070821') OR (one='20090430') OR (one='20180714') OR (one='20070822') OR (one='20180713') OR (one='20070823') OR (one='20180712') OR (one='20070824') OR (one='20180711') OR (one='20070825') OR (one='20180718') OR (one='20070826') OR (one='20180717') OR (one='20070827') OR (one='20180716') OR (one='20070828') OR (one='20180715') OR (one='20070829') OR (one='20180719') OR (one='20180710') OR (one='20070820') OR (one='20180725') OR (one='20090420') OR (one='20180724') OR (one='20090421') OR (one='20180723') OR (one='20090422') OR (one='20180722') OR (one='20180729') OR (one='20180728') OR (one='20100630') OR (one='20180727') OR (one='20180726') OR (one='20090427') OR (one='20090428') OR (one='20090429') OR (one='20090423') OR (one='20180721') OR (one='20090424') OR (one='20180720') OR (one='20070830') OR (one='20090425') OR (one='20070831') OR (one='20090426') OR (one='20181229') OR (one='20181228') OR (one='20181223') OR (one='20181222') OR (one='20181221') OR (one='20181220') OR (one='20181227') OR (one='20181226') OR (one='20181225') OR (one='20181224') OR (one='20181230') OR (one='20181231') OR (one='20181209') OR (one='20181208') OR (one='20181207') OR (one='20181206') OR (one='20160308') OR (one='20160309') OR (one='20160306') OR (one='20160307') OR (one='20160304') OR (one='20181201') OR (one='20160305') OR (one='20160302') OR (one='20160303') OR (one='20181205') OR (one='20160301') OR (one='20181204') OR (one='20181203') OR (one='20181202') OR (one='20181219') OR (one='20181218') OR (one='20181217') OR (one='20140706') OR (one='20140707') OR (one='20140704') OR (one='20140705') OR (one='20140708') OR (one='20140709') OR (one='20140702') OR (one='20140703') OR (one='20140701') OR (one='20181212') OR (one='20181211') OR (one='20181210') OR (one='20181216') OR (one='20181215') OR (one='20181214') OR (one='20181213') OR (one='20101112') OR (one='20101113') OR (one='20101110') OR (one='20101111') OR (one='20101116') " +
            "OR (one='20101117') OR (one='20150920') OR (one='20101114') OR (one='20150921') OR (one='20101115') OR (one='20150922') OR (one='20101118') OR (one='20101119') OR (one='20170531') OR (one='20170530') OR (one='20150923') OR (one='20150924') OR (one='20150925') OR (one='20150926') OR (one='20150927') OR (one='20101120') OR (one='20150928') OR (one='20150929') OR (one='20101101') OR (one='20101102') OR (one='20101105') OR (one='20101106') OR (one='20101103') OR (one='20150910') OR (one='20101104') OR (one='20150911') OR (one='20101109') OR (one='20190131') OR (one='20190130') OR (one='20101107') OR (one='20101108') OR (one='20150912') OR (one='20150913') OR (one='20150914') OR (one='20150915') OR (one='20150916') OR (one='20150917') OR (one='20150918') OR (one='20150919') OR (one='20170511') OR (one='20190120') OR (one='20170510') OR (one='20170513') OR (one='20190122') OR (one='20170512') OR (one='20190121') OR (one='20190124') OR (one='20190123') OR (one='20190126') OR (one='20190125') OR (one='20170519') OR (one='20190128') OR (one='20170518') OR (one='20190127') OR (one='20190129') OR (one='20170515') OR (one='20170514') OR (one='20170517') OR (one='20170516') OR (one='20150930') OR (one='20170522') OR (one='20170521') OR (one='20170524') OR (one='20190111') OR (one='20170523') OR (one='20190110') OR (one='20190113') OR (one='20190112') OR (one='20170520') OR (one='20190115') OR (one='20190114') OR (one='20190117') OR (one='20170529') OR (one='20190116') OR (one='20190119') OR (one='20190118') OR (one='20170526') OR (one='20170525') OR (one='20170528') OR (one='20170527') OR (one='20171020') OR (one='20171022') OR (one='20171021') OR (one='20171028') OR (one='20171027') OR (one='20171029') OR (one='20080129') OR (one='20171024') OR (one='20171023') OR (one='20080127') OR (one='20171026') OR (one='20080128') OR (one='20171025') OR (one='20080125') OR (one='20080126') OR (one='20080123') OR (one='20080124') OR (one='20080121') OR (one='20080122') OR (one='20080120') OR (one='20171031') OR (one='20171030') OR (one='20080130') OR (one='20080131') OR (one='20150101') OR (one='20150102') OR (one='20150103') OR (one='20150104') OR (one='20150105') OR (one='20150106') OR (one='20150107') OR (one='20150108') OR (one='20171006') OR (one='20171005') OR (one='20171008') OR (one='20171007') OR (one='20171002') OR (one='20171001') OR (one='20171004') OR (one='20171003') OR (one='20171009') OR (one='20150109') OR (one='20101123') OR (one='20130503') OR (one='20071206') OR (one='20101124') OR (one='20130504') OR (one='20071207') OR (one='20101121') OR (one='20130505') OR (one='20071208') OR (one='20171011') OR (one='20101122') OR (one='20130506') OR (one='20071209') OR (one='20171010') OR (one='20101127') OR (one='20101128') OR (one='20101125') OR (one='20130501') OR (one='20101126') OR (one='20130502') OR (one='20171017') OR (one='20171016') OR (one='20101129') OR (one='20171019') OR (one='20171018') OR (one='20171013') OR (one='20171012') OR (one='20171015') OR (one='20171014') OR (one='20071201') OR (one='20101130') OR (one='20130507') OR (one='20071202') OR (one='20130508') OR (one='20071203') OR (one='20130509') OR (one='20071204') OR (one='20071205') OR (one='20110905') OR (one='20071217') OR (one='20110906') OR (one='20071218') OR (one='20110907') OR (one='20071219') OR (one='20110908') OR (one='20110909') OR (one='20160328') OR (one='20160329') OR (one='20140717') OR (one='20140718') OR (one='20140715') OR (one='20140716') OR (one='20140719') OR (one='20140710') OR (one='20140713') OR (one='20140714') OR (one='20140711') OR (one='20140712') OR (one='20160326') OR (one='20071210') OR (one='20160327') OR (one='20071211') OR (one='20160324') OR (one='20071212') OR (one='20160325') OR (one='20110901') OR (one='20071213') OR (one='20160322') OR (one='20110902') OR (one='20071214') OR (one='20160323') OR (one='20110903') OR (one='20071215') OR (one='20160320') OR (one='20110904') OR (one='20071216') OR (one='20160321') OR (one='20071228') OR (one='20071229') OR (one='20160319') OR (one='20160317') OR (one='20160318') OR (one='20140728') OR (one='20140729') OR (one='20140726') OR (one='20140727') OR (one='20140720') OR (one='20140721') OR (one='20140724') OR (one='20140725') OR (one='20140722') OR (one='20140723') OR (one='20071220') OR (one='20160315') OR (one='20071221') OR (one='20160316') OR (one='20071222') OR (one='20160313') OR (one='20071223') OR (one='20160314') OR (one='20071224') OR (one='20160311') OR (one='20071225') OR (one='20160312') OR (one='20071226') OR (one='20071227') OR (one='20160310') OR (one='20140731') OR (one='20140730') OR (one='20071230') OR (one='20071231') OR (one='20160330') OR (one='20160331') OR (one='20070722') OR (one='20090331') OR (one='20180813') OR (one='20070723') OR (one='20180812') OR (one='20070724') OR (one='20180811') OR (one='20070725') OR (one='20180810') OR (one='20070726') OR (one='20180817') OR (one='20070727') OR (one='20180816') OR (one='20070728') OR (one='20180815') OR (one='20070729') OR (one='20090330') OR (one='20180814') OR (one='20180819') OR (one='20180818') OR (one='20141205') OR (one='20141204') OR (one='20141203') OR (one='20141202') OR (one='20141209') OR (one='20141208') OR (one='20141207') OR (one='20141206') OR (one='20141201') OR (one='20070720') OR (one='20070721') OR (one='20090320') OR (one='20180824') OR (one='20090321') OR (one='20180823') OR (one='20090322') OR (one='20180822') OR (one='20090323') OR (one='20180821') OR (one='20180828') OR (one='20180827') OR (one='20180826') OR (one='20180825') OR (one='20180829') OR (one='20141216') OR (one='20141215') OR (one='20141214') OR (one='20141213') OR (one='20141219') OR (one='20141218') OR (one='20141217') OR (one='20110930') OR (one='20090328') OR (one='20090329') OR (one='20141212') OR (one='20090324') OR (one='20180820') OR (one='20141211') OR (one='20070730') OR (one='20090325') OR (one='20141210') OR (one='20070731') OR (one='20090326') OR (one='20090327') OR (one='20110927') OR (one='20110928') OR (one='20110929') OR (one='20141227') OR (one='20141226') OR (one='20141225') OR (one='20141224') OR (one='20141229') OR (one='20141228') OR (one='20110920') OR (one='20110921') OR (one='20110922') OR (one='20110923') OR (one='20141223') OR (one='20110924') OR (one='20141222') OR (one='20110925') OR (one='20141221') OR (one='20110926') OR (one='20141220') OR (one='20110916') OR (one='20180802') OR (one='20110917') OR (one='20180801') OR (one='20110918') OR (one='20110919') OR (one='20180806') OR (one='20180805') OR (one='20180804') OR (one='20180803') OR (one='20180809') OR (one='20180808') OR (one='20180807') OR (one='20141230') OR (one='20110910') OR (one='20110911') OR (one='20110912') OR (one='20110913') OR (one='20110914') OR (one='20110915') OR (one='20141231') OR (one='20130514') OR (one='20150123') OR (one='20130515') OR (one='20150124') OR (one='20130516') OR (one='20150125') OR (one='20130517') OR (one='20150126') OR (one='20130510') OR (one='20150127') OR (one='20130511') OR (one='20150128') OR (one='20130512') OR (one='20150129') OR (one='20130513') OR (one='20100702') OR (one='20100703') OR (one='20100701') OR (one='20100706') OR (one='20100707') OR (one='20150120') OR (one='20100704') OR (one='20150121') OR (one='20100705') OR (one='20150122') OR (one='20120308') OR (one='20120309') OR (one='20120306') OR (one='20100708') OR (one='20120307') OR (one='20100709') OR (one='20120304') OR (one='20120305') OR (one='20120302') OR (one='20120303') OR (one='20120301') OR (one='20130518') OR (one='20130519') OR (one='20130525') OR (one='20150112') OR (one='20130526') OR (one='20150113') OR (one='20130527') OR (one='20150114') OR (one='20130528') OR (one='20150115') OR (one='20130521') OR (one='20150116') OR (one='20130522') OR (one='20150117') OR (one='20130523') OR (one='20150118') OR (one='20130524') OR (one='20150119') OR (one='20130520') OR (one='20150110') OR (one='20150111') OR (one='20130529') OR (one='20130530') OR (one='20080109') OR (one='20130531') OR (one='20080107') OR (one='20080108') OR (one='20080105') OR (one='20080106') OR (one='20080103') OR (one='20080104') OR (one='20080101') OR (one='20080102') OR (one='20150130') OR (one='20080118') OR (one='20150131') OR (one='20080119') OR (one='20080116') OR (one='20080117') OR (one='20080114') OR (one='20080115') OR (one='20080112') OR (one='20080113') OR (one='20080110') OR (one='20080111') OR (one='20131009') OR (one='20131006') OR (one='20080929') OR (one='20131005') OR (one='20131008') OR (one='20080927') OR (one='20131007') OR (one='20080928') OR (one='20131002') OR (one='20080925') OR (one='20170610') OR (one='20131001') OR (one='20080926') OR (one='20131004') OR (one='20080923') OR (one='20170612') OR (one='20190221') OR (one='20131003') OR (one='20080924') OR (one='20170611') OR (one='20190220') OR (one='20080921') OR (one='20190223') OR (one='20080922') OR (one='20190222') OR (one='20190225') OR (one='20080920') OR (one='20190224') OR (one='20170618') OR (one='20190227') OR (one='20170617') OR (one='20190226') OR (one='20170619') OR (one='20190228') OR (one='20170614') OR (one='20170613') OR (one='20170616') OR (one='20170615') OR (one='20131017') OR (one='20100731') OR (one='20131016') OR (one='20131019') OR (one='20131018') OR (one='20100730') OR (one='20131013') OR (one='20170621') OR (one='20131012') OR (one='20170620') OR (one='20131015') OR (one='20170623') OR (one='20190210') OR (one='20131014') OR (one='20170622') OR (one='20190212') OR (one='20190211') OR (one='20131011') OR (one='20080930') OR (one='20190214') OR (one='20131010') OR (one='20190213') OR (one='20170629') OR (one='20190216') OR (one='20170628') OR (one='20190215') OR (one='20190218') OR (one='20190217') OR (one='20170625') OR (one='20170624') OR (one='20190219') OR (one='20170627') OR (one='20170626') OR (one='20120331') OR (one='20120330') OR (one='20131028') OR (one='20100720') OR (one='20131027') OR (one='20100721') OR (one='20131029') OR (one='20131024') OR (one='20100724') OR (one='20131023') OR (one='20100725') OR (one='20131026') OR (one='20100722') OR (one='20131025') OR (one='20100723') OR (one='20131020') OR (one='20100728') OR (one='20190201') OR (one='20100729') OR (one='20131022') OR (one='20100726') OR (one='20190203') OR (one='20131021') OR (one='20100727') OR (one='20190202') OR (one='20190205') OR (one='20190204') OR (one='20120328') OR (one='20190207') OR (one='20120329') OR (one='20190206') OR (one='20120326') OR (one='20190209') OR (one='20120327') OR (one='20190208') OR (one='20120324') OR (one='20120325') OR (one='20120322') OR (one='20120323') OR (one='20120320') OR (one='20120321') OR (one='20100710') OR (one='20100713') OR (one='20100714') OR (one='20100711') OR (one='20170601') OR (one='20100712') OR (one='20131031') OR (one='20100717') OR (one='20131030') OR (one='20100718') OR (one='20100715') OR (one='20100716') OR (one='20120319') OR (one='20170607') OR (one='20170606') OR (one='20120317') OR (one='20100719') OR (one='20170609') OR (one='20120318') OR (one='20170608') OR (one='20120315') OR (one='20170603') OR (one='20120316') OR (one='20170602') OR (one='20120313') OR (one='20170605') OR (one='20120314') OR (one='20170604') OR (one='20120311') OR (one='20120312') OR (one='20120310') OR (one='20110101') OR (one='20110102') OR (one='20110103') OR (one='20110104') OR (one='20110105') OR (one='20110106') OR (one='20110107') OR (one='20110108') OR (one='20110109') OR (one='20070701') OR (one='20090310') OR (one='20070702') OR (one='20090311') OR (one='20070703') OR (one='20090312') OR (one='20070704') OR (one='20070705') OR (one='20070706') OR (one='20070707') OR (one='20070708') OR (one='20070709') OR (one='20090317') OR (one='20090318') OR (one='20090319') OR (one='20090313') OR (one='20180831') OR (one='20090314') OR (one='20180830') OR (one='20090315') OR (one='20090316') OR (one='20070711') OR (one='20070712') OR (one='20070713') OR (one='20070714') OR (one='20090301') OR (one='20070715') OR (one='20070716') OR (one='20070717') OR (one='20070718') OR (one='20070719') OR (one='20090306') OR (one='20090307') OR (one='20090308') OR (one='20090309') OR (one='20090302') OR (one='20090303') OR (one='20090304') OR (one='20070710') OR (one='20090305') OR (one='20160409') OR (one='20160407') OR (one='20160408') OR (one='20160405') OR (one='20160406') OR (one='20071130') OR (one='20160403') OR (one='20160404') OR (one='20160401') OR (one='20160402') OR (one='20140805') OR (one='20140806') OR (one='20140803') OR (one='20140804') OR (one='20140809') OR (one='20140807') OR (one='20140808') OR (one='20110130') OR (one='20110131') OR (one='20140801') OR (one='20140802') OR (one='20160429') OR (one='20160427') OR (one='20160428') OR (one='20140816') OR (one='20140817') OR (one='20140814') OR (one='20140815') OR (one='20140818') OR (one='20140819') OR (one='20110120') OR (one='20110121') OR (one='20110122') OR (one='20110123') OR (one='20140812') OR (one='20110124') OR (one='20140813') OR (one='20110125') OR (one='20140810') OR (one='20110126') OR (one='20140811') OR (one='20110127') OR (one='20160425') OR (one='20110128') OR (one='20160426') OR (one='20110129') OR (one='20160423') OR (one='20160424') OR (one='20160421') OR (one='20160422') OR (one='20160420') OR (one='20160418') OR (one='20160419') OR (one='20160416') OR (one='20160417') OR (one='20140827') OR (one='20140828') OR (one='20140825') OR (one='20140826') OR (one='20140829') OR (one='20140820') OR (one='20110110') OR (one='20110111') OR (one='20110112') OR (one='20140823') OR (one='20110113') OR (one='20140824') OR (one='20110114') OR (one='20140821') OR (one='20110115') OR (one='20140822') OR (one='20110116') OR (one='20160414') OR (one='20110117') OR (one='20160415') OR (one='20110118') OR (one='20160412') OR (one='20110119') OR (one='20160413') OR (one='20160410') OR (one='20160411') OR (one='20101231') OR (one='20101222') OR (one='20101223') OR (one='20101220') OR (one='20101221') OR (one='20101226') OR (one='20101227') OR (one='20101224') OR (one='20101225') OR (one='20101228') OR (one='20101229') OR (one='20101230') OR (one='20101211') OR (one='20101212') OR (one='20080909') OR (one='20101210') OR (one='20101215') OR (one='20080907') OR (one='20101216') OR (one='20080908') OR (one='20101213') OR (one='20080905') OR (one='20101214') OR (one='20080906') OR (one='20101219') OR (one='20080903') OR (one='20080904') OR (one='20101217') OR (one='20080901') OR (one='20101218') OR (one='20080902') OR (one='20170630') OR (one='20101201') OR (one='20101204') OR (one='20080918') OR (one='20101205') OR (one='20080919') OR (one='20101202') OR (one='20080916') OR (one='20101203') OR (one='20080917') OR (one='20101208') OR (one='20080914') OR (one='20101209') OR (one='20080915') OR (one='20101206') OR (one='20080912') OR (one='20101207') OR (one='20080913') OR (one='20080910') OR (one='20080911') OR (one='20150201') OR (one='20150202') OR (one='20150203') OR (one='20150204') OR (one='20150205') OR (one='20150206') OR (one='20150207') OR (one='20150208') OR (one='20150209') OR (one='20130602') OR (one='20130603') OR (one='20130604') OR (one='20130605') OR (one='20130601') OR (one='20130606') OR (one='20130607') OR (one='20130608') OR (one='20130609') OR (one='20130613') OR (one='20150222') OR (one='20130614') OR (one='20150223') OR (one='20130615') OR (one='20150224') OR (one='20171121') OR (one='20130616') OR (one='20150225') OR (one='20171120') OR (one='20150226') OR (one='20130610') OR (one='20150227') OR (one='20130611') OR (one='20150228') OR (one='20130612') OR (one='20171127') OR (one='20171126') OR (one='20171129') OR (one='20171128') OR (one='20171123') OR (one='20171122') OR (one='20150220') OR (one='20171125') OR (one='20150221') OR (one='20171124') OR (one='20130617') OR (one='20130618') OR (one='20130619') OR (one='20130624') OR (one='20150211') OR (one='20171130') OR (one='20130625') OR (one='20150212') OR (one='20130626') OR (one='20150213') OR (one='20130627') OR (one='20150214') OR (one='20130620') OR (one='20150215') OR (one='20130621') OR (one='20150216') OR (one='20130622') OR (one='20150217') OR (one='20130623') OR (one='20150218') OR (one='20150210') OR (one='20150219') OR (one='20130628') OR (one='20130629') OR (one='20171105') OR (one='20171104') OR (one='20171107') OR (one='20171106') OR (one='20171101') OR (one='20171103') OR (one='20171102') OR (one='20140830') OR (one='20140831') OR (one='20171109') OR (one='20171108') OR (one='20071107') OR (one='20071108') OR (one='20071109') OR (one='20171110') OR (one='20171116') OR (one='20171115') OR (one='20171118') OR (one='20171117') OR (one='20171112') OR (one='20171111') OR (one='20171114') OR (one='20171113') OR (one='20171119') OR (one='20071101') OR (one='20071102') OR (one='20071103') OR (one='20071104') OR (one='20071105') OR (one='20160430') OR (one='20071106') OR (one='20071118') OR (one='20071119') OR (one='20071110') OR (one='20071111') OR (one='20071112') OR (one='20071113') OR (one='20071114') OR (one='20071115') OR (one='20071116') OR (one='20071117') OR (one='20071129') OR (one='20071120') OR (one='20071121') OR (one='20071122') OR (one='20071123') OR (one='20071124') OR (one='20071125') OR (one='20071126') OR (one='20071127') OR (one='20071128') OR (one='20090210') OR (one='20090211') OR (one='20090212') OR (one='20090213') OR (one='20090218') OR (one='20090219') OR (one='20090214') OR (one='20180930') OR (one='20090215') OR (one='20090216') OR (one='20090217') OR (one='20090201') OR (one='20090202') OR (one='20090207') OR (one='20090208') OR (one='20090209') OR (one='20090203') OR (one='20090204') OR (one='20090205') OR (one='20090206') OR (one='20180912') OR (one='20180911') OR (one='20180910') OR (one='20180916') OR (one='20180915') OR (one='20180914') OR (one='20180913') OR (one='20180919') OR (one='20180918') OR (one='20180917') OR (one='20090221') OR (one='20180923') OR (one='20090222') OR (one='20180922') OR (one='20090223') OR (one='20180921') OR (one='20090224') OR (one='20180920') OR (one='20180927') OR (one='20180926') OR (one='20180925') OR (one='20090220') OR (one='20180924') OR (one='20180929') OR (one='20180928') OR (one='20090225') OR (one='20090226') OR (one='20090227') OR (one='20090228') OR (one='20100820') OR (one='20100823') OR (one='20100824') OR (one='20100821') OR (one='20130630') OR (one='20100822') OR (one='20100827') OR (one='20100828') OR (one='20100825') OR (one='20100826') OR (one='20120429') OR (one='20120427') OR (one='20100829') OR (one='20120428') OR (one='20120425') OR (one='20120426') OR (one='20120423') OR (one='20120424') OR (one='20120421') OR (one='20120422') OR (one='20120420') OR (one='20180901') OR (one='20180905') OR (one='20180904') OR (one='20180903') OR (one='20180902') OR (one='20100812') OR (one='20180909') OR (one='20100813') OR (one='20180908') OR (one='20100810') OR (one='20180907') OR (one='20100811') OR (one='20180906') OR (one='20100816') OR (one='20100817') OR (one='20100814') OR (one='20100815') OR (one='20120418') OR (one='20120419') OR (one='20120416') OR (one='20100818') OR (one='20120417') OR (one='20100819') OR (one='20120414') OR (one='20120415') OR (one='20120412') OR (one='20120413') OR (one='20120410') OR (one='20120411') OR (one='20100801') OR (one='20100802') OR (one='20100805') OR (one='20100806') OR (one='20120409') OR (one='20100803') OR (one='20100804') OR (one='20120407') OR (one='20100809') OR (one='20120408') OR (one='20120405') OR (one='20100807') OR (one='20120406') OR (one='20100808') OR (one='20120403') OR (one='20120404') OR (one='20120401') OR (one='20120402') OR (one='20131127') OR (one='20080808') OR (one='20131126') OR (one='20080809') OR (one='20131129') OR (one='20080806') OR (one='20131128') OR (one='20080807') OR (one='20131123') OR (one='20080804') OR (one='20170731') OR (one='20131122') OR (one='20080805') OR (one='20170730') OR (one='20131125') OR (one='20080802') OR (one='20131124') OR (one='20080803') OR (one='20080801') OR (one='20131121') OR (one='20131120') OR (one='20080819') OR (one='20080817') OR (one='20080818') OR (one='20080815') OR (one='20080816') OR (one='20080813') OR (one='20190331') OR (one='20080814') OR (one='20190330') OR (one='20131130') OR (one='20080811') OR (one='20080812') OR (one='20080810') OR (one='20080828') OR (one='20080829') OR (one='20080826') OR (one='20080827') OR (one='20080824') OR (one='20170711') OR (one='20190320') OR (one='20080825') OR (one='20170710') OR (one='20080822') OR (one='20190322') OR (one='20080823') OR (one='20190321') OR (one='20080820') OR (one='20190324') OR (one='20080821') OR (one='20190323') OR (one='20170717') OR (one='20190326') OR (one='20170716') OR (one='20190325') OR (one='20170719') OR (one='20190328') OR (one='20170718') OR (one='20190327') OR (one='20170713') OR (one='20170712') OR (one='20190329') OR (one='20170715') OR (one='20170714') OR (one='20100830') OR (one='20100831') OR (one='20170720') OR (one='20170722') OR (one='20170721') OR (one='20190311') OR (one='20190310') OR (one='20080831') OR (one='20190313') OR (one='20190312') OR (one='20170728') OR (one='20190315') OR (one='20080830') OR (one='20170727') OR (one='20190314') OR (one='20190317') OR (one='20170729') OR (one='20190316') OR (one='20170724') OR (one='20190319') OR (one='20170723') OR (one='20190318') OR (one='20170726') OR (one='20170725') OR (one='20120430') OR (one='20190302') OR (one='20190301') OR (one='20190304') OR (one='20190303') OR (one='20110220') OR (one='20190306') OR (one='20110221') OR (one='20190305') OR (one='20110222') OR (one='20190308') OR (one='20110223') OR (one='20190307') OR (one='20110224') OR (one='20110225') OR (one='20190309') OR (one='20110226') OR (one='20110227') OR (one='20110228') OR (one='20170706') OR (one='20170705') OR (one='20170708') OR (one='20110210') OR (one='20170707') OR (one='20110211') OR (one='20170702') OR (one='20110212') OR (one='20170701') OR (one='20110213') OR (one='20170704') OR (one='20110214') OR (one='20170703') OR (one='20110215') OR (one='20110216') OR (one='20110217') OR (one='20110218') OR (one='20110219') OR (one='20170709') OR (one='20131109') OR (one='20131108') OR (one='20131105') OR (one='20131104') OR (one='20131107') OR (one='20131106') OR (one='20131101') OR (one='20131103') OR (one='20131102') OR (one='20110201') OR (one='20110202') OR (one='20110203') OR (one='20110204') OR (one='20110205') OR (one='20110206') OR (one='20110207') OR (one='20110208') OR (one='20110209') OR (one='20131119') OR (one='20131116') OR (one='20131115') OR (one='20131118') OR (one='20131117') OR (one='20131112') OR (one='20131111') OR (one='20131114') OR (one='20131113') OR (one='20131110') OR (one='20071019') OR (one='20160528') OR (one='20160529') OR (one='20160526') OR (one='20160527') OR (one='20140915') OR (one='20140916') OR (one='20140913') OR (one='20140914') OR (one='20140919') OR (one='20140917') OR (one='20140918') OR (one='20140911') OR (one='20180130') OR (one='20140912') OR (one='20140910') OR (one='20071010') OR (one='20071011') OR (one='20160524') OR (one='20071012') OR (one='20160525') OR (one='20071013') OR (one='20160522') OR (one='20071014') OR (one='20160523') OR (one='20180131') OR (one='20071015') OR (one='20160520') OR (one='20071016') OR (one='20160521') OR (one='20071017') OR (one='20071018') OR (one='20160519') OR (one='20160517') OR (one='20160518') OR (one='20160515') OR (one='20160516') OR (one='20140926') OR (one='20140927') OR (one='20140924') OR (one='20140925') OR (one='20140928') OR (one='20140929') OR (one='20140922') OR (one='20140923') OR (one='20140920') OR (one='20071020') OR (one='20140921') OR (one='20071021') OR (one='20071022') OR (one='20160513') OR (one='20071023') OR (one='20160514') OR (one='20071024') OR (one='20160511') OR (one='20071025') OR (one='20160512') OR (one='20071026') OR (one='20071027') OR (one='20160510') OR (one='20071028') OR (one='20071029') OR (one='20180119') OR (one='20180118') OR (one='20180117') OR (one='20140930') OR (one='20071030') OR (one='20071031') OR (one='20180112') OR (one='20180111') OR (one='20180110') OR (one='20180116') OR (one='20180115') OR (one='20180114') OR (one='20180113') OR (one='20180129') OR (one='20180128') OR (one='20180123') OR (one='20180122') OR (one='20180121') OR (one='20180120') OR (one='20160531') OR (one='20180127') OR (one='20180126') OR (one='20180125') OR (one='20160530') OR (one='20180124') OR (one='20180109') OR (one='20180108') OR (one='20180107') OR (one='20180106') OR (one='20180101') OR (one='20180105') OR (one='20180104') OR (one='20180103') OR (one='20180102') OR (one='20160508') OR (one='20160509') OR (one='20160506') OR (one='20160507') OR (one='20160504') OR (one='20160505') OR (one='20160502') OR (one='20160503') OR (one='20160501') OR (one='20140904') OR (one='20140905') OR (one='20140902') OR (one='20140903') OR (one='20140908') OR (one='20140909') OR (one='20140906') OR (one='20140907') OR (one='20140901') OR (one='20130712') OR (one='20150321') OR (one='20130713') OR (one='20150322') OR (one='20130714') OR (one='20150323') OR (one='20130715') OR (one='20150324') OR (one='20150325') OR (one='20150326') OR (one='20130710') OR (one='20150327') OR (one='20130711') OR (one='20150328') OR (one='20081218') OR (one='20081219') OR (one='20081216') OR (one='20150320') OR (one='20081217') OR (one='20081214') OR (one='20081215') OR (one='20081212') OR (one='20081213') OR (one='20081210') OR (one='20081211') OR (one='20150329') OR (one='20130716') OR (one='20130717') OR (one='20130718') OR (one='20130719') OR (one='20130723') OR (one='20150310') OR (one='20130724') OR (one='20150311') OR (one='20130725') OR (one='20150312') OR (one='20130726') OR (one='20150313') OR (one='20150314') OR (one='20130720') OR (one='20150315') OR (one='20130721') OR (one='20150316') OR (one='20130722') OR (one='20150317') OR (one='20081229') OR (one='20081227') OR (one='20081228') OR (one='20081225') OR (one='20081226') OR (one='20081223') OR (one='20081224') OR (one='20081221') OR (one='20081222') OR (one='20081220') OR (one='20150318') OR (one='20150319') OR (one='20130727') OR (one='20130728') OR (one='20130729') OR (one='20130730') OR (one='20130731') OR (one='20081230') OR (one='20081231') OR (one='20150330') OR (one='20150331') OR (one='20171220') OR (one='20171226') OR (one='20171225') OR (one='20171228') OR (one='20171227') OR (one='20171222') OR (one='20171221') OR (one='20171224') OR (one='20171223') OR (one='20171229') OR (one='20171231') OR (one='20171230') OR (one='20150301') OR (one='20150302') OR (one='20150303') OR (one='20150304') OR (one='20150305') OR (one='20150306') OR (one='20171204') OR (one='20171203') OR (one='20171206') OR (one='20171205') OR (one='20171202') OR (one='20171201') OR (one='20171208') OR (one='20171207') OR (one='20171209') OR (one='20150307') OR (one='20150308') OR (one='20150309') OR (one='20130701') OR (one='20071008') OR (one='20130702') OR (one='20071009') OR (one='20130703') OR (one='20130704') OR (one='20171215') OR (one='20171214') OR (one='20171217') OR (one='20171216') OR (one='20171211') OR (one='20171210') OR (one='20171213') OR (one='20171212') OR (one='20171219') OR (one='20171218') OR (one='20130709') OR (one='20071001') OR (one='20071002') OR (one='20071003') OR (one='20130705') OR (one='20071004') OR (one='20130706') OR (one='20071005') OR (one='20130707') OR (one='20071006') OR (one='20130708') OR (one='20071007') OR (one='20100901') OR (one='20100904') OR (one='20100905') OR (one='20120508') OR (one='20100902') OR (one='20120509') OR (one='20100903') OR (one='20120506') OR (one='20100908') OR (one='20140115') OR (one='20120507') OR (one='20100909') OR (one='20140116') OR (one='20120504') OR (one='20100906') OR (one='20140113') OR (one='20120505') OR (one='20100907') OR (one='20140114') OR (one='20120502') OR (one='20140119') OR (one='20120503') OR (one='20140117') OR (one='20120501') OR (one='20140118') OR (one='20140111') OR (one='20140112') OR (one='20140110') OR (one='20140126') OR (one='20140127') OR (one='20140124') OR (one='20140125') OR (one='20140128') OR (one='20140129') OR (one='20140122') OR (one='20140123') OR (one='20140120') OR (one='20140121') OR (one='20090111') OR (one='20090112') OR (one='20090113') OR (one='20090114') OR (one='20090110') OR (one='20090119') OR (one='20140130') OR (one='20090115') OR (one='20090116') OR (one='20140131') OR (one='20090117') OR (one='20090118') OR (one='20090101') OR (one='20090102') OR (one='20090103') OR (one='20090108') OR (one='20090109') OR (one='20090104') OR (one='20090105') OR (one='20090106') OR (one='20090107') OR (one='20090130') OR (one='20090131') OR (one='20161012') OR (one='20161011') OR (one='20161010') OR (one='20161019') OR (one='20161018') OR (one='20161017') OR (one='20161016') OR (one='20161015') OR (one='20161014') OR (one='20161013') OR (one='20090122') OR (one='20090123') OR (one='20090124') OR (one='20090125') OR (one='20100930') OR (one='20090120') OR (one='20090121') OR (one='20161001') OR (one='20120531') OR (one='20161009') OR (one='20161008') OR (one='20161007') OR (one='20120530') OR (one='20161006') OR (one='20090126') OR (one='20161005') OR (one='20090127') OR (one='20161004') OR (one='20090128') OR (one='20161003') OR (one='20090129') OR (one='20161002') OR (one='20100922') OR (one='20100923') OR (one='20100920') OR (one='20100921') OR (one='20100926') OR (one='20100927') OR (one='20100924') OR (one='20100925') OR (one='20120528') OR (one='20120529') OR (one='20120526') OR (one='20100928') OR (one='20120527') OR (one='20100929') OR (one='20161031') OR (one='20120524') OR (one='20161030') OR (one='20120525') OR (one='20120522') OR (one='20120523') OR (one='20120520') OR (one='20120521') OR (one='20100911') OR (one='20100912') OR (one='20081209') OR (one='20100910') OR (one='20100915') OR (one='20081207') OR (one='20100916') OR (one='20081208') OR (one='20120519') OR (one='20100913') OR (one='20081205') OR (one='20100914') OR (one='20081206') OR (one='20120517') OR (one='20100919') OR (one='20140104') OR (one='20081203') OR (one='20161023') OR (one='20120518') OR (one='20140105') OR (one='20081204') OR (one='20161022') OR (one='20120515') OR (one='20100917') OR (one='20140102') OR (one='20081201') OR (one='20161021') OR (one='20120516') OR (one='20100918') OR (one='20140103') OR (one='20081202') OR (one='20161020') OR (one='20120513') OR (one='20140108') OR (one='20120514') OR (one='20140109') OR (one='20120511') OR (one='20140106') OR (one='20120512') OR (one='20140107') OR (one='20120510') OR (one='20161029') OR (one='20161028') OR (one='20161027') OR (one='20140101') OR (one='20161026') OR (one='20161025') OR (one='20161024') OR (one='20110301') OR (one='20110302') OR (one='20110303') OR (one='20110304') OR (one='20110305') OR (one='20110306') OR (one='20110307') OR (one='20110308') OR (one='20110309') OR (one='20080709') OR (one='20080707') OR (one='20080708') OR (one='20080705') OR (one='20170830') OR (one='20080706') OR (one='20080703') OR (one='20080704') OR (one='20170831') OR (one='20080701') OR (one='20080702') OR (one='20080718') OR (one='20080719') OR (one='20080716') OR (one='20080717') OR (one='20080714') OR (one='20190430') OR (one='20080715') OR (one='20080712') OR (one='20080713') OR (one='20080710') OR (one='20080711') OR (one='20131208') OR (one='20131207') OR (one='20131209') OR (one='20131204') OR (one='20131203') OR (one='20131206') OR (one='20080729') OR (one='20131205') OR (one='20080727') OR (one='20080728') OR (one='20131202') OR (one='20080725') OR (one='20170810') OR (one='20131201') OR (one='20080726') OR (one='20080723') OR (one='20190421') OR (one='20080724') OR (one='20190420') OR (one='20080721') OR (one='20190423') OR (one='20080722') OR (one='20190422') OR (one='20170816') OR (one='20190425') OR (one='20080720') OR (one='20170815') OR (one='20190424') OR (one='20170818') OR (one='20190427') OR (one='20170817') OR (one='20190426') OR (one='20170812') OR (one='20190429') OR (one='20170811') OR (one='20190428') OR (one='20170814') OR (one='20170813') OR (one='20170819') OR (one='20131219') OR (one='20131218') OR (one='20131215') OR (one='20131214') OR (one='20131217') OR (one='20131216') OR (one='20131211') OR (one='20131210') OR (one='20131213') OR (one='20170821') OR (one='20131212') OR (one='20170820') OR (one='20190410') OR (one='20190412') OR (one='20190411') OR (one='20080730') OR (one='20170827') OR (one='20190414') OR (one='20080731') OR (one='20170826') OR (one='20190413') OR (one='20110330') OR (one='20170829') OR (one='20190416') OR (one='20110331') OR (one='20170828') OR (one='20190415') OR (one='20170823') OR (one='20190418') OR (one='20170822') OR (one='20190417') OR (one='20170825') OR (one='20170824') OR (one='20190419') OR (one='20131229') OR (one='20131226') OR (one='20131225') OR (one='20131228') OR (one='20131227') OR (one='20131222') OR (one='20131221') OR (one='20131224') OR (one='20131223') OR (one='20131220') OR (one='20190401') OR (one='20190403') OR (one='20190402') OR (one='20190405') OR (one='20110320') OR (one='20190404') OR (one='20110321') OR (one='20190407') OR (one='20110322') OR (one='20190406') OR (one='20110323') OR (one='20190409') OR (one='20110324') OR (one='20190408') OR (one='20110325') OR (one='20110326') OR (one='20110327') OR (one='20110328') OR (one='20110329') OR (one='20131231') OR (one='20131230') OR (one='20170805') OR (one='20170804') OR (one='20170807') OR (one='20170806') OR (one='20110310') OR (one='20170801') OR (one='20110311') OR (one='20110312') OR (one='20170803') OR (one='20110313') OR (one='20170802') OR (one='20110314') OR (one='20110315') OR (one='20110316') OR (one='20110317') OR (one='20110318') OR (one='20170809') OR (one='20110319') OR (one='20170808') OR (one='20100101') OR (one='20100104') OR (one='20100105') OR (one='20100102') OR (one='20100103') OR (one='20100108') OR (one='20100109') OR (one='20100106') OR (one='20100107') OR (one='20121031') OR (one='20121030') OR (one='20121028') OR (one='20121029') OR (one='20121026') OR (one='20121027') OR (one='20121024') OR (one='20121025') OR (one='20121022') OR (one='20160630') OR (one='20121023') OR (one='20121020') OR (one='20121021') OR (one='20090919') OR (one='20121019') OR (one='20090915') OR (one='20090916') OR (one='20121017') OR (one='20090917') OR (one='20121018') OR (one='20090918') OR (one='20121015') OR (one='20090911') OR (one='20121016') OR (one='20090912') OR (one='20121013') OR (one='20090913') OR (one='20121014') OR (one='20090914') OR (one='20121011') OR (one='20121012') OR (one='20121010') OR (one='20090910') OR (one='20090908') OR (one='20090909') OR (one='20121008') OR (one='20090904') OR (one='20121009') OR (one='20090905') OR (one='20121006') OR (one='20090906') OR (one='20121007') OR (one='20090907') OR (one='20121004') OR (one='20121005') OR (one='20090901') OR (one='20121002') OR (one='20090902') OR (one='20121003') OR (one='20090903') OR (one='20121001') OR (one='20160609') OR (one='20180219') OR (one='20180218') OR (one='20160607') OR (one='20180217') OR (one='20160608') OR (one='20180216') OR (one='20160605') OR (one='20160606') OR (one='20160603') OR (one='20160604') OR (one='20160601') OR (one='20180211') OR (one='20160602') OR (one='20180210') OR (one='20180215') OR (one='20090930') OR (one='20180214') OR (one='20180213') OR (one='20180212') OR (one='20100131') OR (one='20180228') OR (one='20180227') OR (one='20090926') OR (one='20090927') OR (one='20090928') OR (one='20090929') OR (one='20090922') OR (one='20180222') OR (one='20090923') OR (one='20180221') OR (one='20090924') OR (one='20180220') OR (one='20090925') OR (one='20180226') OR (one='20180225') OR (one='20090920') OR (one='20180224') OR (one='20090921') OR (one='20180223') OR (one='20100122') OR (one='20100123') OR (one='20100120') OR (one='20160629') OR (one='20100121') OR (one='20100126') OR (one='20160627') OR (one='20100127') OR (one='20160628') OR (one='20100124') OR (one='20160625') OR (one='20100125') OR (one='20160626') OR (one='20100128') OR (one='20100129') OR (one='20160623') OR (one='20160624') OR (one='20160621') OR (one='20160622') OR (one='20100130') OR (one='20160620') OR (one='20100111') OR (one='20180208') OR (one='20100112') OR (one='20180207') OR (one='20160618') OR (one='20180206') OR (one='20100110') OR (one='20160619') OR (one='20180205') OR (one='20100115') OR (one='20160616') OR (one='20100116') OR (one='20160617') OR (one='20100113') OR (one='20160614') OR (one='20100114') OR (one='20160615') OR (one='20180209') OR (one='20100119') OR (one='20100117') OR (one='20100118') OR (one='20160612') OR (one='20160613') OR (one='20160610') OR (one='20160611') OR (one='20180204') OR (one='20180203') OR (one='20180202') OR (one='20180201') OR (one='20130830') OR (one='20130831') OR (one='20081108') OR (one='20081109') OR (one='20081106') OR (one='20150430') OR (one='20081107') OR (one='20081104') OR (one='20081105') OR (one='20081102') OR (one='20081103') OR (one='20081101') OR (one='20081119') OR (one='20081117') OR (one='20081118') OR (one='20081115') OR (one='20161111') OR (one='20081116') OR (one='20161110') OR (one='20081113') OR (one='20081114') OR (one='20081111') OR (one='20081112') OR (one='20081110') OR (one='20161119') OR (one='20161118') OR (one='20161117') OR (one='20161116') OR (one='20161115') OR (one='20161114') OR (one='20161113') OR (one='20161112') OR (one='20161109') OR (one='20081128') OR (one='20081129') OR (one='20081126') OR (one='20081127') OR (one='20081124') OR (one='20081125') OR (one='20081122') OR (one='20081123') OR (one='20081120') OR (one='20081121') OR (one='20161108') OR (one='20161107') OR (one='20161106') OR (one='20161105') OR (one='20161104') OR (one='20161103') OR (one='20161102') OR (one='20161101') OR (one='20150401') OR (one='20150402') OR (one='20150403') OR (one='20150404') OR (one='20150405') OR (one='20150406') OR (one='20150407') OR (one='20081130') OR (one='20150408') OR (one='20150409') OR (one='20130801') OR (one='20130802') OR (one='20130803') OR (one='20130808') OR (one='20130809') OR (one='20130804') OR (one='20130805') OR (one='20130806') OR (one='20130807') OR (one='20130811') OR (one='20150420') OR (one='20130812') OR (one='20150421') OR (one='20130813') OR (one='20150422') OR (one='20130814') OR (one='20150423') OR (one='20150424') OR (one='20150425') OR (one='20150426') OR (one='20130810') OR (one='20150427') OR (one='20130819') OR (one='20150428') OR (one='20150429') OR (one='20130815') OR (one='20130816') OR (one='20130817') OR (one='20130818') OR (one='20130822') OR (one='20130823') OR (one='20150410') OR (one='20130824') OR (one='20150411') OR (one='20130825') OR (one='20150412') OR (one='20150413') OR (one='20150414') OR (one='20130820') OR (one='20150415') OR (one='20130821') OR (one='20150416') OR (one='20150417') OR (one='20150418') OR (one='20150419') OR (one='20130826') OR (one='20130827') OR (one='20130828') OR (one='20130829') OR (one='20120629') OR (one='20120627') OR (one='20190502') OR (one='20120628') OR (one='20190501') OR (one='20120625') OR (one='20190504') OR (one='20120626') OR (one='20190503') OR (one='20120623') OR (one='20190506') OR (one='20120624') OR (one='20190505') OR (one='20120621') OR (one='20190508') OR (one='20120622') OR (one='20190507') OR (one='20120620') OR (one='20190509') OR (one='20120618') OR (one='20120619') OR (one='20120616') OR (one='20170904') OR (one='20120617') OR (one='20170903') OR (one='20120614') OR (one='20170906') OR (one='20120615') OR (one='20170905') OR (one='20120612') OR (one='20120613') OR (one='20120610') OR (one='20170902') OR (one='20120611') OR (one='20170901') OR (one='20170908') OR (one='20170907') OR (one='20170909') OR (one='20120609') OR (one='20120607') OR (one='20120608') OR (one='20120605') OR (one='20120606') OR (one='20120603') OR (one='20120604') OR (one='20120601') OR (one='20120602') OR (one='20161130') OR (one='20140209') OR (one='20140203') OR (one='20161122') OR (one='20140204') OR (one='20161121') OR (one='20140201') OR (one='20161120') OR (one='20140202') OR (one='20140207') OR (one='20140208') OR (one='20140205') OR (one='20140206') OR (one='20161129') OR (one='20161128') OR (one='20161127') OR (one='20161126') OR (one='20161125') OR (one='20161124') OR (one='20161123') OR (one='20140214') OR (one='20140215') OR (one='20140212') OR (one='20140213') OR (one='20140218') OR (one='20140219') OR (one='20140216') OR (one='20140217') OR (one='20140210') OR (one='20140211') OR (one='20140225') OR (one='20140226') OR (one='20140223') OR (one='20140224') OR (one='20140227') OR (one='20140228') OR (one='20120630') OR (one='20140221') OR (one='20140222') OR (one='20140220') OR (one='20110420') OR (one='20110421') OR (one='20110422') OR (one='20110423') OR (one='20110424') OR (one='20110425') OR (one='20110426') OR (one='20110427') OR (one='20110428') OR (one='20110429') OR (one='20180307') OR (one='20180306') OR (one='20180305') OR (one='20180304') OR (one='20180309') OR (one='20180308') OR (one='20110410') OR (one='20110411') OR (one='20110412') OR (one='20110413') OR (one='20110414') OR (one='20110415') OR (one='20110416') OR (one='20110417') OR (one='20180303') OR (one='20110418') OR (one='20180302') OR (one='20110419') OR (one='20180301') OR (one='20160708') OR (one='20160709') OR (one='20160706') OR (one='20160707') OR (one='20160704') OR (one='20160705') OR (one='20160702') OR (one='20160703') OR (one='20110401') OR (one='20110402') OR (one='20110403') OR (one='20160701') OR (one='20110404') OR (one='20110405') OR (one='20110406') OR (one='20110407') OR (one='20110408') OR (one='20110409') OR (one='20080608') OR (one='20080609') OR (one='20080606') OR (one='20080607') OR (one='20080604') OR (one='20080605') OR (one='20170930') OR (one='20080602') OR (one='20080603') OR (one='20080601') OR (one='20121118') OR (one='20121119') OR (one='20121116') OR (one='20121117') OR (one='20121114') OR (one='20121115') OR (one='20121112') OR (one='20121113') OR (one='20121110') OR (one='20121111') OR (one='20080619') OR (one='20080617') OR (one='20080618') OR (one='20080615') OR (one='20080616') OR (one='20080613') OR (one='20190531') OR (one='20080614') OR (one='20190530') OR (one='20080611') OR (one='20080612') OR (one='20080610') OR (one='20121109')"))
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

    def "List definition metadata valid and invalid sortBy" () {
        given:
        def qName = "embedded-hive-metastore/smoke_db/dm"
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
