/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.metacat

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.base.Throwables
import com.netflix.metacat.client.Client
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.client.api.MetacatV1
import com.netflix.metacat.client.api.PartitionV1
import com.netflix.metacat.client.api.ResolverV1
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.exception.MetacatAlreadyExistsException
import com.netflix.metacat.common.exception.MetacatBadRequestException
import com.netflix.metacat.common.exception.MetacatNotFoundException
import com.netflix.metacat.common.exception.MetacatNotSupportedException
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import feign.Logger
import feign.RetryableException
import org.apache.hadoop.hive.metastore.Warehouse
import org.joda.time.Instant
import org.skyscreamer.jsonassert.JSONAssert
import spock.lang.Specification
import spock.lang.Stepwise
import spock.lang.Unroll

@Stepwise
@Unroll
class MetacatFunctionalSpec extends Specification {
    public static MetacatV1 api
    public static PartitionV1 partitionApi
    public static ResolverV1 resolverApi
    public static final MetacatJson metacatJson = MetacatJsonLocator.INSTANCE
    public static final long BATCH_ID = System.currentTimeSeconds()
    public static final int timediff = 24 * 3600

    def setupSpec() {
        def httpPort = System.properties['metacat_http_port']?.toString()?.trim()
        assert httpPort, 'Required system property "metacat_http_port" is not set'
        def client = Client.builder()
            .withHost("http://localhost:$httpPort")
            .withDataTypeContext('pig')
            .withUserName('metacat-test')
            .withClientAppName('metacat-test')
            .withLogLevel(Logger.Level.FULL)
            .build()
        api = client.api
        partitionApi = client.partitionApi
        resolverApi = client.resolverApi
        TestCatalogs.resetAll()
    }


    def 'getCatalogName: existing catalogs'() {
        when:
        def catalogNames = api.getCatalogNames()

        then:
        catalogNames.size() >= TestCatalogs.ALL.size()
        TestCatalogs.ALL.every { catalog ->
            def match = catalogNames.find { it.catalogName == catalog.name }
            assert match, "Unable to find catalog ${catalog.name}"
            assert match.connectorName == catalog.type

            return true
        }
    }

    def 'createCatalog: #catalogType fails'() {
        when:
        api.createCatalog(new CreateCatalogDto(type: catalogType))

        then:
        thrown(MetacatNotSupportedException)

        where:
        catalogName | catalogType
        'mysqlhost' | 'mysql'
        'hivehost'  | 'hive'
    }

    def 'getCatalog: fails for nonexistent catalog'() {
        when:
        api.getCatalog(catalogName)

        then:
        thrown(MetacatNotFoundException)

        where:
        catalogName = 'nonexistent_catalog'
    }

    def 'getCatalog: #catalog.name exists and has existing databases #catalog.preExistingDatabases'() {
        when:
        def catResponse = api.getCatalog(catalog.name)
        def catResponseJson = TestUtilities.toJsonString(catResponse)

        then:
        String str = "{name:{catalogName:$catalog.name,qualifiedName:$catalog.name},type:$catalog.type}"
        JSONAssert.assertEquals(str, catResponseJson, false)
        catResponse.name == QualifiedName.ofCatalog(catalog.name)
        catResponse.type == catalog.type
        catResponse.databases.containsAll(catalog.preExistingDatabases*.databaseName)

        where:
        catalog << TestCatalogs.ALL
    }

    def 'updateCatalog: fails for nonexistent catalog of type #catalogType'() {
        when:
        api.updateCatalog(catalogName, new CreateCatalogDto(type: catalogType))

        then:
        thrown(MetacatNotFoundException)

        where:
        catalogName           | catalogType
        'nonexistent_catalog' | 'mysql'
        'nonexistent_catalog' | 'hive'
    }

    def 'updateCatalog: can update #catalog.name of type #catalog.type'() {
        given:
        def now = new Date().time
        def metadata = metacatJson.parseJsonObject("""{
        "test_time": ${now},
        "batch_id": ${BATCH_ID}
}""")

        when:
        def catResponse = api.getCatalog(catalog.name)

        then:
        !catResponse.definitionMetadata

        when:
        api.updateCatalog(catalog.name, new CreateCatalogDto(type: catalog.type, definitionMetadata: metadata))
        catResponse = api.getCatalog(catalog.name)

        then:
        catResponse.definitionMetadata == metadata

        where:
        catalog << TestCatalogs.ALL
    }

    def "create test_db"() {
        given:
        ObjectNode metadata = metacatJson.parseJsonObject('{"objectField": {}}')
        def dto = new DatabaseCreateRequestDto(definitionMetadata: metadata)
        String databaseName = "test_db_${catalog.name.replace('-', '_')}".toString()

        when:
        def catalogResponse = api.getCatalog(catalog.name)

        then:
        if (!catalogResponse.databases.contains(databaseName)) {
            api.createDatabase(catalog.name, databaseName, dto)
        } else {
            println "test_db already exists in $catalog.name. Skipping create test_db"
        }

        where:
        catalog << TestCatalogs.getCanCreateDatabase(TestCatalogs.ALL)
    }


    def 'createDatabase: nonexistent_catalog #metadataMessage fails'() {
        given:
        def dto = new DatabaseCreateRequestDto(definitionMetadata: metadata)

        when:
        api.createDatabase('nonexistent_catalog', 'does not matter', dto)

        then:
        thrown(MetacatNotFoundException)

        where:
        metadata << [null, metacatJson.emptyObjectNode()]
        metadataMessage = metadata == null ? 'without metadata' : 'with metadata'
    }

    def 'createDatabase: not support for #catalog.name of type #catalog.type'() {
        given:
        def dto = new DatabaseCreateRequestDto()

        when:
        api.createDatabase(catalog.name, 'does_not_matter', dto)

        then:
        thrown(MetacatNotSupportedException)

        where:
        catalog << TestCatalogs.getCanNotCreateDatabase(TestCatalogs.ALL)
    }

    def 'createDatabase: can create a database in #catalog.name without metadata'() {
        given:
        ObjectNode metadata = null
        def dto = new DatabaseCreateRequestDto(definitionMetadata: metadata)
        String databaseName = "db_no_metadata_${catalog.name.replace('-', '_')}_$BATCH_ID".toString()

        when:
        def catalogResponse = api.getCatalog(catalog.name)

        then:
        !catalogResponse.databases.contains(databaseName)

        when:
        api.createDatabase(catalog.name, databaseName, dto)
        catalogResponse = api.getCatalog(catalog.name)

        then:
        catalogResponse.type == catalog.type
        catalogResponse.name == QualifiedName.ofCatalog(catalog.name)
        catalogResponse.databases.contains(databaseName)

        when:
        def database = api.getDatabase(catalog.name, databaseName, false)

        then:
        database.type == catalog.type
        database.name == QualifiedName.ofDatabase(catalog.name, databaseName)
        database.definitionMetadata == null

        when:
        database = api.getDatabase(catalog.name, databaseName, true)
        catalog.createdDatabases << QualifiedName.ofDatabase(catalog.name, databaseName)

        then:
        database.type == catalog.type
        database.name == QualifiedName.ofDatabase(catalog.name, databaseName)
        database.definitionMetadata == metadata
        database.tables.empty

        where:
        catalog << TestCatalogs.getCanCreateDatabase(TestCatalogs.ALL)
    }

    def 'createDatabase: can create a database in #catalog.name with metadata'() {
        given:
        ObjectNode metadata = metacatJson.parseJsonObject('{"objectField": {}}')
        def dto = new DatabaseCreateRequestDto(definitionMetadata: metadata)
        String databaseName = "db_metadata_${catalog.name.replace('-', '_')}_$BATCH_ID".toString()

        when:
        def catalogResponse = api.getCatalog(catalog.name)

        then:
        !catalogResponse.databases.contains(databaseName)

        when:
        api.createDatabase(catalog.name, databaseName, dto)
        catalogResponse = api.getCatalog(catalog.name)

        then:
        catalogResponse.type == catalog.type
        catalogResponse.name == QualifiedName.ofCatalog(catalog.name)
        catalogResponse.databases.contains(databaseName)

        when:
        def database = api.getDatabase(catalog.name, databaseName, false)

        then:
        database.type == catalog.type
        database.name == QualifiedName.ofDatabase(catalog.name, databaseName)
        database.definitionMetadata == null

        when:
        database = api.getDatabase(catalog.name, databaseName, true)
        catalog.createdDatabases << QualifiedName.ofDatabase(catalog.name, databaseName)

        then:
        database.type == catalog.type
        database.name == QualifiedName.ofDatabase(catalog.name, databaseName)
        database.definitionMetadata == metadata
        database.tables.empty

        where:
        catalog << TestCatalogs.getCanCreateDatabase(TestCatalogs.ALL)
    }

    def 'createDatabase: fails when calling create with existing database #name'() {
        when:
        def catalog = api.getCatalog(name.catalogName)

        then:
        catalog.databases.contains(name.databaseName)

        when:
        api.createDatabase(name.catalogName, name.databaseName, new DatabaseCreateRequestDto())

        then:
        thrown(MetacatAlreadyExistsException)
        //to create stub data
        where:
        name << TestCatalogs.getAllDatabases(TestCatalogs.getCanCreateDatabase(TestCatalogs.ALL))
    }

    def 'getDatabase: has tables for preexisting database #name'() {
        given:
        def sakilaTables = ['actor', 'address', 'city']
        def worldTables = ['city', 'country', 'countrylanguage']
        def real_estateTables = ['apartments', 'houses']
        def billsTables = ['bills_compress', 'bills_nc']

        when:
        def database = api.getDatabase(name.catalogName, name.databaseName, true)

        then:
        if (name.databaseName == 'world') {
            assert database.tables.containsAll(worldTables)
        } else if (name.databaseName == 'sakila') {
            assert database.tables.containsAll(sakilaTables)
        } else if (name.databaseName == 'real_estate') {
            assert database.tables.containsAll(real_estateTables)
        } else if (name.databaseName == 'bills') {
            assert database.tables.containsAll(billsTables)
        } else if (name.databaseName == 'public' || name.databaseName == 'pg_catalog') {
            assert name.catalogName.contains("postgresql")
        } else {
            throw new IllegalStateException("Unknown database: ${name.databaseName}")
        }
        database.definitionMetadata == null

        where:
        name << TestCatalogs.getPreExistingDatabases(TestCatalogs.ALL)
    }

    def 'getDatabase: for created database #name with metadata'() {
        when:
        def database = api.getDatabase(name.catalogName, name.databaseName, true)

        then:
        database.tables.empty
        if (name.databaseName.contains('db_metadata')) {
            assert database.definitionMetadata.fieldNames().collect().contains('objectField')
        } else {
            assert database.definitionMetadata == null
        }

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.ALL)
    }

    def 'getDatabase: for created database #name without metadata'() {
        when:
        def database = api.getDatabase(name.catalogName, name.databaseName, false)

        then:
        database.tables.empty
        database.definitionMetadata == null

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.ALL)
    }

    def "getTable: Test get table for #name"() {
        given:
        def catalogName = name.catalogName
        def databaseName = name.databaseName
        def tableName = name.tableName

        when:
        def table = api.getTable(catalogName, databaseName, tableName, true, true, true)

        then:
        table != null

        where:
        name << TestCatalogs.getAllTables(TestCatalogs.ALL)
    }

    def 'createTable: should fail for #catalog where it is not supported'() {
        given:
        def databaseName = (catalog.preExistingDatabases + catalog.createdDatabases).first().databaseName
        def tableName = "table_$BATCH_ID".toString()
        def dto = new TableDto(
            name: QualifiedName.ofTable(catalog.name, databaseName, tableName),
            serde: new StorageDto(
                owner: 'metacat-test'
            )
        )

        when:
        def database = api.getDatabase(catalog.name, databaseName, false)

        then:
        !database.tables.contains(tableName)

        when:
        api.createTable(catalog.name, databaseName, tableName, dto)

        then:
        thrown(MetacatNotSupportedException)

        where:
        catalog << TestCatalogs.getCanNotCreateTable(TestCatalogs.ALL)
    }

    def 'createTable: should fail for nonexistent catalog zz_#catalog.name'() {
        given:
        def databaseName = "created_database_${catalog.name.replace('-', '_')}".toString()
        def tableName = "table_$BATCH_ID".toString()
        def dto = new TableDto(
            name: QualifiedName.ofTable('zz_' + catalog.name, databaseName, tableName),
            serde: new StorageDto(
                owner: 'metacat-test'
            )
        )

        when:
        api.createTable('zz_' + catalog.name, databaseName, tableName, dto)

        then:
        thrown(MetacatNotFoundException)

        where:
        catalog << TestCatalogs.getCanCreateTable(TestCatalogs.ALL)
    }

    def 'createTable: should fail for nonexistent database #catalog.name/missing_database'() {
        given:
        def databaseName = "missing_database_${catalog.name.replace('-', '_')}".toString()
        def tableName = "table_$BATCH_ID".toString()
        def dto = new TableDto(
            name: QualifiedName.ofTable(catalog.name, databaseName, tableName),
            serde: new StorageDto(
                owner: 'metacat-test'
            )
        )

        when:
        api.createTable(catalog.name, databaseName, tableName, dto)

        then:
        def e = thrown(MetacatNotFoundException)
        def rc = e.cause ? Throwables.getRootCause(e) : null
        def expectedMessage = "DATABASE '" + catalog.name + "/" + databaseName + "' not found"
        e.message.contains(expectedMessage) || rc?.message?.contains(expectedMessage)

        where:
        catalog << TestCatalogs.getCanCreateTable(TestCatalogs.ALL)
    }

    def 'createTable: inside #name'() {
        given:
        def tableName = "table_$BATCH_ID".toString()
        def now = new Date()
        def databaseName = "test_db_${catalog.name.replace('-', '_')}".toString()

        def dataUri = "file:/tmp/${catalog.name}/${databaseName}/${tableName}".toString()
        ObjectNode definitionMetadata = metacatJson.parseJsonObject('{"objectField": {}}')
        ObjectNode dataMetadata = metacatJson.emptyObjectNode().put('data_field', 4)
        def dto = new TableDto(
            name: QualifiedName.ofTable(catalog.name, databaseName, tableName),
            audit: new AuditDto(
                createdBy: 'createdBy',
                createdDate: now
            ),
            serde: new StorageDto(
                owner: 'metacat-test',
                inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                parameters: [
                    'serialization.format': '1'
                ],
                uri: dataUri
            ),
            definitionMetadata: definitionMetadata,
            dataMetadata: dataMetadata,
            fields: [
                new FieldDto(
                    comment: 'added 1st - partition key',
                    name: 'field1',
                    pos: 0,
                    type: 'boolean',
                    partition_key: true
                ),
                new FieldDto(
                    comment: 'added 2st',
                    name: 'field2',
                    pos: 1,
                    type: 'boolean',
                    partition_key: false
                ),
                new FieldDto(
                    comment: 'added 3rd, a single char partition key to test that use case',
                    name: 'p',
                    pos: 2,
                    type: 'boolean',
                    partition_key: true
                ),
                new FieldDto(
                    comment: 'added 4st',
                    name: 'field4',
                    pos: 3,
                    type: 'boolean',
                    partition_key: false
                ),
            ]
        )

        when:
        def database = api.getDatabase(catalog.name, databaseName, false)

        then:
        !database.tables.contains(tableName)

        when:
        api.createTable(catalog.name, databaseName, tableName, dto)
        database = api.getDatabase(catalog.name, databaseName, false)

        then:
        database.tables.contains(tableName)

        when:
        def resovlerRep = resolverApi.resolveByUri(false, new ResolveByUriRequestDto(uri: dataUri))
        then:
        !resovlerRep.tables.empty

        when:
        resovlerRep = resolverApi.resolveByUri(true,
            new ResolveByUriRequestDto(uri: "file:/tmp/${catalog.name}/${databaseName}/".toString()))
        then:
        !resovlerRep.tables.empty

        when:
        def table = api.getTable(catalog.name, databaseName, tableName, true, true, true)
        def name = QualifiedName.ofTable(catalog.name, databaseName, tableName)
        def tableJson = TestUtilities.toJsonString(table)
        def expectedhivedataMetadata = "{\"dataMetadata\":{\"data_field\":4},\"definitionMetadata\":{\"objectField\":{}},\"fields\":[{\"comment\":\"added 2st\",\"name\":\"field2\",\"partition_key\":false,\"pos\":0,\"source_type\":\"boolean\",\"type\":\"boolean\",\"isNullable\":null,\"size\":null,\"defaultValue\":null,\"isSortKey\":null,\"isIndexKey\":null},{\"comment\":\"added 4st\",\"name\":\"field4\",\"partition_key\":false,\"pos\":1,\"source_type\":\"boolean\",\"type\":\"boolean\",\"isNullable\":null,\"size\":null,\"defaultValue\":null,\"isSortKey\":null,\"isIndexKey\":null},{\"comment\":\"added 1st - partition key\",\"name\":\"field1\",\"partition_key\":true,\"pos\":2,\"source_type\":\"boolean\",\"type\":\"boolean\",\"isNullable\":null,\"size\":null,\"defaultValue\":null,\"isSortKey\":null,\"isIndexKey\":null},{\"comment\":\"added 3rd, a single char partition key to test that use case\",\"name\":\"p\",\"partition_key\":true,\"pos\":3,\"source_type\":\"boolean\",\"type\":\"boolean\",\"isNullable\":null,\"size\":null,\"defaultValue\":null,\"isSortKey\":null,\"isIndexKey\":null}]}"
        def expecteds3dataMetadata = "{\"dataMetadata\":{\"data_field\":4},\"definitionMetadata\":{\"objectField\":{}},\"fields\":[{\"comment\":\"added 2st\",\"name\":\"field2\",\"partition_key\":false,\"pos\":1,\"source_type\":null,\"type\":\"boolean\",\"isNullable\":null,\"size\":null,\"defaultValue\":null,\"isSortKey\":null,\"isIndexKey\":null},{\"comment\":\"added 4st\",\"name\":\"field4\",\"partition_key\":false,\"pos\":3,\"source_type\":null,\"type\":\"boolean\",\"isNullable\":null,\"size\":null,\"defaultValue\":null,\"isSortKey\":null,\"isIndexKey\":null},{\"comment\":\"added 1st - partition key\",\"name\":\"field1\",\"partition_key\":true,\"pos\":0,\"source_type\":null,\"type\":\"boolean\",\"isNullable\":null,\"size\":null,\"defaultValue\":null,\"isSortKey\":null,\"isIndexKey\":null},{\"comment\":\"added 3rd, a single char partition key to test that use case\",\"name\":\"p\",\"partition_key\":true,\"pos\":2,\"source_type\":null,\"type\":\"boolean\",\"isNullable\":null,\"size\":null,\"defaultValue\":null,\"isSortKey\":null,\"isIndexKey\":null}]}"
        def serdeStr = "{\"serde\":{\"inputFormat\":\"org.apache.hadoop.mapred.TextInputFormat\",\"outputFormat\":\"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\"owner\":\"metacat-test\",\"parameters\":{\"serialization.format\":\"1\"},\"serdeInfoParameters\":{},\"serializationLib\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\",\"uri\":\"file:/tmp/${catalog.name}/$databaseName/$tableName\"},\"partition_keys\":[\"field1\",\"p\"],\"dataExternal\":true}"
        def tablemetadataStr = "{\"metadata\":{\"EXTERNAL\":\"TRUE\"},\"name\":{\"catalogName\":$catalog.name,\"databaseName\":$databaseName,\"qualifiedName\":\"${catalog.name}/$databaseName/$tableName\",\"tableName\":\"$tableName\"}}"

        then:
        //s3 connector maintains the order of the field, but doesn't have the soruce type
        if (catalog.type == "s3") {
            JSONAssert.assertEquals(expecteds3dataMetadata, tableJson, false)
        }
        //hive connector puts all the partition fields at the end
        if (catalog.type == 'hive') {
            JSONAssert.assertEquals(expectedhivedataMetadata, tableJson, false)
            JSONAssert.assertEquals(serdeStr, tableJson, false)
            JSONAssert.assertEquals(tablemetadataStr, tableJson, false)
        }

        table.serde.owner == 'metacat-test'
        table.name.catalogName == catalog.name
        table.name.databaseName == databaseName
        table.name.tableName == tableName
        table.definitionMetadata == definitionMetadata
        table.dataMetadata == dataMetadata
        table.fields.find { it.name == 'field1' }.partition_key
        !table.fields.find { it.name == 'field2' }.partition_key
        table.fields.find { it.name == 'p' }.partition_key
        !table.fields.find { it.name == 'field4' }.partition_key
        // Hive partitions keys are always sorted to the end of the fields.

        if (TestCatalogs.findByQualifiedName(name).partitionKeysAppearLast) {
            assert table.fields*.name == ['field2', 'field4', 'field1', 'p']
            table.fields*.partition_key == [false, false, true, true]
        } else {
            assert table.fields*.name == ['field1', 'field2', 'p', 'field4']
            table.fields*.partition_key == [true, false, true, false]
        }
        table.fields*.type == ['boolean', 'boolean', 'boolean', 'boolean']
        TestCatalogs.findByQualifiedName(name).createdTables << table.name

        where:
        catalog << TestCatalogs.getCanCreateTable(TestCatalogs.ALL)
    }

    def 'createTable: test_table'() {
        given:
        def tableName = "test_table".toString()
        def now = new Date()
        def databaseName = "test_db_${catalog.name.replace('-', '_')}".toString()
        def dataUri = "file:/tmp/${catalog.name}/${databaseName}/${tableName}".toString()
        ObjectNode definitionMetadata = metacatJson.parseJsonObject('{"objectField": {}}')
        ObjectNode dataMetadata = metacatJson.emptyObjectNode().put('data_field', 4)
        def dto = new TableDto(
            name: QualifiedName.ofTable(catalog.name, databaseName, tableName),
            audit: new AuditDto(
                createdBy: 'createdBy',
                createdDate: now
            ),
            serde: new StorageDto(
                owner: 'metacat-test',
                inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                parameters: [
                    'serialization.format': '1'
                ],
                uri: dataUri
            ),
            definitionMetadata: definitionMetadata,
            dataMetadata: dataMetadata,
            fields: [
                new FieldDto(
                    comment: 'added 1st - partition key',
                    name: 'field1',
                    pos: 0,
                    type: 'boolean',
                    partition_key: true
                ),
                new FieldDto(
                    comment: 'added 2st',
                    name: 'field2',
                    pos: 1,
                    type: 'boolean',
                    partition_key: false
                ),
                new FieldDto(
                    comment: 'added 3rd, a single char partition key to test that use case',
                    name: 'p',
                    pos: 2,
                    type: 'boolean',
                    partition_key: true
                ),
                new FieldDto(
                    comment: 'added 4st',
                    name: 'field4',
                    pos: 3,
                    type: 'boolean',
                    partition_key: false
                ),
            ]
        )

        when:
        def database = api.getDatabase(catalog.name, databaseName, false)

        then:
        if (!database.tables.contains(tableName)) {
            api.createTable(catalog.name, databaseName, tableName, dto)
        } else {
            println "test_table already exist in $catalog.name/$databaseName Skipping create test_table"
        }

        where:
        catalog << TestCatalogs.getCanCreateTable(TestCatalogs.ALL)
    }


    def 'updateTable: #name'() {
        given:
        def now = new Date()

        when:
        def tableName = "test_table"
        def databaseName = "test_db_${catalog.name.replace('-', '_')}".toString()
        def table = api.getTable(catalog.name, databaseName, tableName, true, true, true)
        def originalDefinitionMetadata = table.definitionMetadata
        def mergedDefinitionMetadata = metacatJson.emptyObjectNode().put('now', now.toString())
        if (originalDefinitionMetadata?.toString()) {
            mergedDefinitionMetadata = originalDefinitionMetadata.deepCopy()
            mergedDefinitionMetadata.put('now', now.toString())
        }
        def originalDataMetadata = table.dataMetadata
        def origDataUri = table.dataUri
        def updatedDataUri = origDataUri + '_updated_uri'

        and:
        table.definitionMetadata = metacatJson.emptyObjectNode().put('now', now.toString())
        table.dataMetadata = metacatJson.emptyObjectNode().put('data now', now.toString())
        table.serde.uri = updatedDataUri

        and:
        api.updateTable(catalog.name, databaseName, tableName, table)
        table = api.getTable(catalog.name, databaseName, tableName, true, true, true)

        then: 'saving should merge or insert metadata'
        table.definitionMetadata == mergedDefinitionMetadata

        and: 'since the uri changed the data metadata should be only the new metadata'
        table.dataMetadata == metacatJson.emptyObjectNode().put('data now', now.toString())

        and: 'should update the dataUri'
        table.dataUri == updatedDataUri

        when: 'when the data uri is changed back'
        table.serde.uri = origDataUri
        table.definitionMetadata = null
        table.dataMetadata = null
        api.updateTable(catalog.name, databaseName, tableName, table)
        table = api.getTable(catalog.name, databaseName, tableName, true, true, true)

        then: 'the old data metadata should be back'
        table.dataMetadata == originalDataMetadata
        table.serde.uri == origDataUri

        then: 'but the definition metadata should be retained'
        table.definitionMetadata == mergedDefinitionMetadata

        where:
        catalog << TestCatalogs.getCanCreateTable(TestCatalogs.ALL)
    }

    def 'savePartition: should fail when given a null or missing value for #pname'() {
        given:
        def name = pname as QualifiedName
        def dbname = "test_db_${name.catalogName.replace('-', '_')}".toString()

        def dataUri = "file:/tmp/${name.catalogName}/$dbname/test_table/${name.partitionName}".toString()

        def request = new PartitionsSaveRequestDto(
            partitions: [
                new PartitionDto(
                    name: name,
                    serde: new StorageDto(
                        inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                        outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        uri: dataUri
                    ),
                )
            ]
        )

        when:
        partitionApi.savePartitions(name.catalogName, name.databaseName, name.tableName, request)

        then:
        def e = thrown(MetacatBadRequestException)
        def message = e.cause ? Throwables.getRootCause(e).message : e.message
        message.contains('Partition value') || message.contains('Partition name')
        message.contains('cannot be null or empty') || message.contains('is invalid')

        where:
        pname << TestCatalogs.getCanCreateTable(TestCatalogs.ALL)
            .collect { tname ->
            [
                [field1: 'null', p: 'valid'],
                [field1: '', p: 'valid'],
                [field1: 'valid', p: 'null'],
                [field1: 'valid', p: ''],
                [field1: 'null', p: 'null'],
                [field1: 'null', p: ''],
                [field1: '', p: 'null'],
                [field1: '', p: ''],
            ].collect {
                String unescapedPartitionName = "field1=${it.field1}/p=${it.p}".toString()
                QualifiedName.ofPartition(tname.name, "test_db_${tname.name.replace('-', '_')}".toString(), "test_table", unescapedPartitionName)
            }
        }.flatten()
    }

    def 'savePartition: can add partitions to #args.name'() {
        given:
        def name = args.name as QualifiedName
        def escapedName = args.escapedName as QualifiedName
        def now = Instant.now().toDate()
        def dataUri = "file:/tmp/${name.catalogName}/${name.databaseName}/${name.tableName}/${escapedName.partitionName}".toString()
        def definitionMetadata = metacatJson.emptyObjectNode().put('part_def_field', Long.MAX_VALUE)
        def dataMetadata = metacatJson.emptyObjectNode().put('part_data_field', Integer.MIN_VALUE)
        def tableMetadata = metacatJson.emptyObjectNode().put('table_def_field', now.time)

        def request = new PartitionsSaveRequestDto(
            definitionMetadata: tableMetadata,
            partitions: [
                new PartitionDto(
                    name: name,
                    definitionMetadata: definitionMetadata,
                    dataMetadata: dataMetadata,
                    dataExternal: true,
                    audit: new AuditDto(
                        createdDate: now,
                        lastModifiedDate: now
                    ),
                    serde: new StorageDto(
                        inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                        outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        uri: dataUri
                    ),
                )
            ]
        )

        def resolveByUridto = new ResolveByUriRequestDto(uri: dataUri)

        when:
        def keys = partitionApi.getPartitionKeys(name.catalogName, name.databaseName, name.tableName, null, null, null, null, null)

        then:
        !keys.contains(name.partitionName)
        !keys.contains(escapedName.partitionName)

        when:
        def response = partitionApi.savePartitions(name.catalogName, name.databaseName, name.tableName, request)

        then:
        response.added.contains(escapedName.partitionName)
        response.updated.empty

        when:
        def resovlerRep = resolverApi.resolveByUri(false, resolveByUridto)
        then:
        !resovlerRep.partitions.empty

        when:
        resovlerRep = resolverApi.resolveByUri(true,
            new ResolveByUriRequestDto(uri: "file:/tmp/${name.catalogName}/${name.databaseName}/${name.tableName}".toString()))
        then:
        !resovlerRep.partitions.empty

        when:
        resolverApi.isUriUsedMoreThanOnce(false, resolveByUridto)
        then:
        thrown(MetacatNotFoundException)

        when:
        keys = partitionApi.getPartitionKeys(name.catalogName, name.databaseName, name.tableName, null, null, null, null, null)

        then:
        keys.contains(escapedName.partitionName)

        when:
        def allPartitions = partitionApi.getPartitions(name.catalogName, name.databaseName, name.tableName, null, null, null, null, null, true)
        def savedPartition = allPartitions?.find { it.name == escapedName }

        def partitionJson = TestUtilities.toJsonString(savedPartition)
        String dataMetadataStr = "{\"dataMetadata\":{\"part_data_field\":${Integer.MIN_VALUE}}}"
        String definitionMetadataStr = "{\"definitionMetadata\":{\"part_def_field\":${Long.MAX_VALUE}}}"
        def expected = "{\"dataMetadata\":{\"part_data_field\":${Integer.MIN_VALUE}},\"definitionMetadata\":{\"part_def_field\":${Long.MAX_VALUE}},\"name\":{\"catalogName\":\"${name.catalogName}\",\"partitionName\":\"${escapedName.partitionName}\",\"databaseName\":\"${name.databaseName}\",\"qualifiedName\":\"${name.catalogName}/${name.databaseName}/${name.tableName}/${escapedName.partitionName}\",\"tableName\":\"${name.tableName}\"},\"serde\":{\"inputFormat\":\"org.apache.hadoop.mapred.TextInputFormat\",\"outputFormat\":\"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\"serializationLib\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\",\"uri\":\"file:/tmp/${name.catalogName}/${name.databaseName}/${name.tableName}/${escapedName.partitionName}\"},\"dataExternal\":true}"
        then:
        JSONAssert.assertEquals(dataMetadataStr, partitionJson, false)
        JSONAssert.assertEquals(definitionMetadataStr, partitionJson, false)
        JSONAssert.assertEquals(expected, partitionJson, false)
        allPartitions
        savedPartition
        savedPartition.name == escapedName
        TestUtilities.dateCloseEnough(savedPartition.audit.createdDate, now, timediff)
        TestUtilities.dateCloseEnough(savedPartition.audit.lastModifiedDate, now, timediff)
        savedPartition.serde.uri == dataUri
        savedPartition.definitionMetadata.get('part_def_field').longValue() == Long.MAX_VALUE
        savedPartition.dataMetadata.get('part_data_field').intValue() == Integer.MIN_VALUE

        when:
        def table = api.getTable(name.catalogName, name.databaseName, name.tableName, true, true, true)
        then:
        table.definitionMetadata.get('table_def_field').longValue() == now.time

        when: "nothing is changed on the partitions"
        request = new PartitionsSaveRequestDto(
            partitions: [
                new PartitionDto(
                    name: name,
                    serde: new StorageDto(
                        uri: dataUri
                    ),
                )
            ]
        )
        response = partitionApi.savePartitions(name.catalogName, name.databaseName, name.tableName, request)

        then: "nothing is added or updated"
        response.added.empty
        response.updated.empty

        when: "something is updated on the partitions"
        request.partitions.first().serde.uri = "${dataUri}_new_uri"
        request.partitions.first().definitionMetadata = metacatJson.parseJsonObject('{"updated_field": 1}')
        request.partitions.first().dataMetadata = metacatJson.parseJsonObject('{"new_entry":true}')
        request.alterIfExists = Boolean.TRUE
        response = partitionApi.savePartitions(name.catalogName, name.databaseName, name.tableName, request)

        then: "the partition is updated"
        response.added.empty
        response.updated.contains(escapedName.partitionName)

        when:
        allPartitions = partitionApi.getPartitions(name.catalogName, name.databaseName, name.tableName, null, null, null, null, null, true)
        savedPartition = allPartitions?.find { it.name == escapedName }
        partitionJson = TestUtilities.toJsonString(savedPartition)

        def strJson = "{\"name\":{\"catalogName\":\"${name.catalogName}\",\"partitionName\":\"${escapedName.partitionName}\",\"databaseName\":\"${name.databaseName}\",\"qualifiedName\":\"${name.catalogName}/${name.databaseName}/${name.tableName}/${escapedName.partitionName}\",\"tableName\":\"${name.tableName}\"},\"serde\":{\"inputFormat\":\"org.apache.hadoop.mapred.TextInputFormat\",\"outputFormat\":\"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\",\"serializationLib\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\",\"uri\":\"${dataUri}_new_uri\"},\"dataExternal\":true}"
        dataMetadataStr = "{\"dataMetadata\":{\"new_entry\":true}}"
        definitionMetadataStr = "{\"definitionMetadata\":{\"part_def_field\":${Long.MAX_VALUE},\"updated_field\":1}}"

        then:
        JSONAssert.assertEquals(dataMetadataStr, partitionJson, false)
        JSONAssert.assertEquals(definitionMetadataStr, partitionJson, false)
        JSONAssert.assertEquals(strJson, partitionJson, false)


        allPartitions
        savedPartition
        savedPartition.name == escapedName
        TestUtilities.dateCloseEnough(savedPartition.audit.createdDate, now, timediff)
        TestUtilities.dateCloseEnough(savedPartition.audit.lastModifiedDate, now, timediff)
        savedPartition.serde.uri == "${dataUri}_new_uri".toString()
        savedPartition.definitionMetadata.get('updated_field').intValue() == 1
        savedPartition.definitionMetadata.get('part_def_field').longValue() == Long.MAX_VALUE

        and: 'since the uri changed there should only be the new metadata'
        savedPartition.dataMetadata == metacatJson.parseJsonObject('{"new_entry":true}')
        TestCatalogs.findByQualifiedName(escapedName).createdPartitions << escapedName

        where:
        args << TestCatalogs.getCreatedTables(TestCatalogs.ALL)
            .collect { tname ->
            [
                [field1: 'lower', p: 'UPPER'],
                [field1: 'UPPER', p: 'UPPER'],
                [field1: 'UPPER', p: 'lower'],
                [field1: 'lower', p: 'lower'],
                [field1: 'camelCase', p: 'camelCase'],
                [field1: 'string with space', p: 'valid'],
                [field1: 'valid', p: 'string with space'],
            ].collect {
                String unescapedPartitionName = "field1=${it.field1}/p=${it.p}".toString()
                String escapedPartitionName = unescapedPartitionName
                return [
                    name       : QualifiedName.ofPartition(tname.catalogName, tname.databaseName, tname.tableName, unescapedPartitionName),
                    escapedName: QualifiedName.ofPartition(tname.catalogName, tname.databaseName, tname.tableName, escapedPartitionName),
                ]
            }
        }.flatten()
    }

    def 'test partition filtering expressions in #name'() {
        given:
        def tableName = "table_part_filtering_$BATCH_ID".toString()
        def dto = new TableDto(
            name: QualifiedName.ofTable(name.catalogName, name.databaseName, tableName),
            fields: [
                new FieldDto(
                    name: 'pk1',
                    type: 'chararray',
                    partition_key: true
                ),
                new FieldDto(
                    name: 'pk2',
                    type: 'long',
                    partition_key: true
                ),
                new FieldDto(
                    name: 'pk3',
                    type: 'long',
                    partition_key: true
                ),
                new FieldDto(
                    name: 'field4',
                    type: 'chararray',
                    partition_key: false
                ),
            ]
        )

        when:
        def database = api.getDatabase(name.catalogName, name.databaseName, false)

        then:
        !database.tables.contains(tableName)

        when:
        api.createTable(name.catalogName, name.databaseName, tableName, dto)
        database = api.getDatabase(name.catalogName, name.databaseName, false)

        then:
        database.tables.contains(tableName)

        when:
        TestCatalogs.findByQualifiedName(name).createdTables << dto.name
        PartitionsSaveRequestDto partitionsSaveRequestDto = new PartitionsSaveRequestDto(partitions: [])
        (0..15).each { i ->
            String pk1 = i % 2 == 0 ? 'even' : 'odd'
            String pk2 = Integer.valueOf(i)
            String pk3 = Integer.valueOf(i % 2)
            partitionsSaveRequestDto.partitions.add(new PartitionDto(
                name: QualifiedName.ofPartition(name.catalogName, name.databaseName, tableName, "pk1=${pk1}/pk2=${pk2}/pk3=${pk3}"),
                serde: new StorageDto(
                    uri: "file:/tmp/${name.catalogName}/${name.databaseName}/${tableName}/${Random.newInstance().nextInt(Integer.MAX_VALUE)}".toString()
                ),
            ))
        }
        def resp = partitionApi.savePartitions(name.catalogName, name.databaseName, tableName, partitionsSaveRequestDto)

        then:
        resp.added.size() == 16

        and:
        def f = { String filter ->
            partitionApi.getPartitions(name.catalogName, name.databaseName, tableName, filter, null, null, null, null, null)
        }.memoize()

        when:
        f('pk1=even')

        then: 'an exception is thrown when using a non quoted string'
        thrown(Exception)

        when:
        def result
        try {
            result = f('pk3="1"')
        } catch (Throwable t) {
            result = t
        }

        then:
        if (TestCatalogs.findByCatalogName(name.catalogName).validateFilterExpressionBasedOnPartitionKeyType) {
            assert result instanceof Throwable,
                'an exception should be thrown when using a quoted long when types are verified'
        } else {
            assert result.size() == 8
            assert result.every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=1') }
        }

        expect:
        f('').size() == 16
        f('pk1="even"').size() == 8
        f('pk1="even"').every { PartitionDto partition -> partition.name.partitionName.startsWith('pk1=even/') }
//        f('pk1=="even"').size() == 8
        f('pk3=1').size() == 8
        f('pk3=1').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=1') }
//        f('pk3==1').size() == 8
        f('pk3!=1').size() == 8
        f('pk3!=1').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=0') }
        f('pk3 <> 1').size() == 8
        f('pk3 <> 1').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=0') }
        f('pk1="even" or pk1="odd"').size() == 16
        f('pk1="even" or pk1="odd"').every { PartitionDto partition -> partition.name.partitionName.startsWith('pk1=') }
        f('pk1="even" OR pk1="odd"').size() == 16
        f('pk1="even" OR pk1="odd"').every { PartitionDto partition -> partition.name.partitionName.startsWith('pk1=') }
        f('pk1="even" or pk1="does_not_exist"').size() == 8
        f('pk1="even" or pk1="does_not_exist"').every { PartitionDto partition -> partition.name.partitionName.startsWith('pk1=even/') }
        f('pk1="even" OR pk1="does_not_exist"').size() == 8
        f('pk1="even" OR pk1="does_not_exist"').every { PartitionDto partition -> partition.name.partitionName.startsWith('pk1=even/') }
        f('pk1="even" and pk1="odd"').size() == 0
        f('pk1="even" AND pk1="odd"').size() == 0
        f('pk1="even" and pk3=1').size() == 0
        f('pk1="even" AND pk3=1').size() == 0
        f('pk1="odd" and pk3=0').size() == 0
        f('pk1="odd" AND pk3=0').size() == 0
        f('pk1="even" and pk3=0').size() == 8
        f('pk1="even" and pk3=0').every { PartitionDto partition -> partition.name.partitionName.startsWith('pk1=even/') && partition.name.partitionName.endsWith('/pk3=0') }
        f('pk1="even" AND pk3=0').size() == 8
        f('pk1="even" AND pk3=0').every { PartitionDto partition -> partition.name.partitionName.startsWith('pk1=even/') && partition.name.partitionName.endsWith('/pk3=0') }
        f('pk1="odd" and pk3=1').size() == 8
        f('pk1="odd" and pk3=1').every { PartitionDto partition -> partition.name.partitionName.startsWith('pk1=odd/') && partition.name.partitionName.endsWith('/pk3=1') }
        f('pk1="odd" AND pk3=1').size() == 8
        f('pk1="odd" AND pk3=1').every { PartitionDto partition -> partition.name.partitionName.startsWith('pk1=odd/') && partition.name.partitionName.endsWith('/pk3=1') }
        f('pk1="odd" and pk2=7').size() == 1
        f('pk1="odd" and pk2=7').first().name.partitionName == 'pk1=odd/pk2=7/pk3=1'
        f('pk1="odd" AND pk2=7').size() == 1
        f('pk1="odd" AND pk2=7').first().name.partitionName == 'pk1=odd/pk2=7/pk3=1'
        f('pk1="odd" and pk2=8').size() == 0
        f('pk1="odd" AND pk2=8').size() == 0
        f('pk1="even" and pk2=7').size() == 0
        f('pk1="even" AND pk2=7').size() == 0
        f('pk1="even" and pk2=8').size() == 1
        f('pk1="even" and pk2=8').first().name.partitionName == 'pk1=even/pk2=8/pk3=0'
        f('pk1="even" AND pk2=8').size() == 1
        f('pk1="even" AND pk2=8').first().name.partitionName == 'pk1=even/pk2=8/pk3=0'
        f('pk3=1 and pk2=7').size() == 1
        f('pk3=1 and pk2=7').first().name.partitionName == 'pk1=odd/pk2=7/pk3=1'
        f('pk3=1 AND pk2=7').size() == 1
        f('pk3=1 AND pk2=7').first().name.partitionName == 'pk1=odd/pk2=7/pk3=1'
        f('pk3=1 and pk2=8').size() == 0
        f('pk3=1 AND pk2=8').size() == 0
        f('pk3=0 and pk2=7').size() == 0
        f('pk3=0 AND pk2=7').size() == 0
        f('pk3=0 and pk2=8').size() == 1
        f('pk3=0 and pk2=8').first().name.partitionName == 'pk1=even/pk2=8/pk3=0'
        f('pk3=0 AND pk2=8').size() == 1
        f('pk3=0 AND pk2=8').first().name.partitionName == 'pk1=even/pk2=8/pk3=0'
        f('(pk3 = 0) OR (pk3 = 1)').size() == 16
        f('(pk3 = 0) AND (pk3 = 1)').size() == 0
        f('pk3 > -1').size() == 16
        f('-1 < pk3').size() == 16
        f('pk3 <= -1').size() == 0
        f('-1 >= pk3').size() == 0
        f('pk3 != 1').size() == 8
        f('pk3 != 1').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=0') }
        f('1 != pk3').size() == 8
        f('1 != pk3').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=0') }
        f('pk3 < 1').size() == 8
        f('pk3 < 1').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=0') }
        f('1 > pk3').size() == 8
        f('1 > pk3').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=0') }
        f('pk3 < 0').size() == 0
        f('0 > pk3').size() == 0
        f('pk3 <= 0').size() == 8
        f('pk3 <= 0').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=0') }
        f('0 >= pk3').size() == 8
        f('0 >= pk3').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=0') }
        f('pk3 > 0').size() == 8
        f('pk3 > 0').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=1') }
        f('0 < pk3').size() == 8
        f('0 < pk3').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=1') }
        f('pk3 >= 1').size() == 8
        f('pk3 >= 1').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=1') }
        f('1 <= pk3').size() == 8
        f('1 <= pk3').every { PartitionDto partition -> partition.name.partitionName.endsWith('/pk3=1') }
        f('pk3 > 2').size() == 0
        f('pk3 >= 2').size() == 0
        f('2 < pk3').size() == 0
        f('2 <= pk3').size() == 0
        f('pk2 between 5 and 8').size() == 4
        f('pk2 between 5 and 7').size() == 3
        f('pk2 between 5 and 6').size() == 2
        f('pk2 between 5 and 5').size() == 1

        where:
        name << TestCatalogs.getAllDatabases(TestCatalogs.getCanCreateTable(TestCatalogs.ALL))
    }

    def 'createMView: #name'() {
        given:
        def viewName = "v1"
        def viewQName = QualifiedName.ofView(name.catalogName, name.databaseName, name.tableName, viewName)

        when:
        def database = api.getDatabase(name.catalogName, name.databaseName, false)

        then:
        database.tables.contains(name.tableName)

        when:
        api.createMView(name.catalogName, name.databaseName, name.tableName, viewName, false, null)
        def views = api.getMViews(name.catalogName, name.databaseName, name.tableName)
        def viewNames = views.collect { it.getName() }

        then:
        viewNames.contains(viewQName)

        when:
        def view = api.getMView(name.catalogName, name.databaseName, name.tableName, viewName)

        then:
        view.name == viewQName

        where:
        name << TestCatalogs.getCreatedTables(TestCatalogs.getCanDeleteTable(TestCatalogs.ALL))
    }

    def 'deletePartition: #name'() {
        given:
        def spec = Warehouse.makeSpecFromName(name.partitionName)
        String unescapedPartitionName = "field1=${spec.field1}/p=${spec.p}".toString()

        when:
        def keys = partitionApi.getPartitionKeys(name.catalogName, name.databaseName, name.tableName, null, null, null, null, null)

        then:
        keys.contains(name.partitionName)

        when:
        partitionApi.deletePartitions(name.catalogName, name.databaseName, name.tableName, [unescapedPartitionName])
        keys = partitionApi.getPartitionKeys(name.catalogName, name.databaseName, name.tableName, null, null, null, null, null)

        then:
        !keys.contains(name.partitionName)
        !keys.contains(unescapedPartitionName)
        TestCatalogs.findByQualifiedName(name).createdPartitions.remove(name)

        where:
        name << TestCatalogs.getCreatedPartitions(TestCatalogs.ALL)
    }

    def 'deleteDatabase: #name fails if the db is not empty'() {
        when:
        def catalog = api.getCatalog(name.catalogName)

        then:
        catalog.databases.contains(name.databaseName)

        when:
        api.deleteDatabase(name.catalogName, name.databaseName)

        then:
        def exception = thrown(RetryableException)
        exception.message.contains('is not empty')

        when:
        catalog = api.getCatalog(name.catalogName)

        then:
        catalog.databases.contains(name.databaseName)

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.getCanDeleteDatabase(TestCatalogs.ALL))
    }

    def 'deleteTable: #name'() {
        when:
        def database = api.getDatabase(name.catalogName, name.databaseName, false)

        then:
        database.tables.contains(name.tableName)

        when:
        api.getTable(name.catalogName, name.databaseName, name.tableName, false, false, false)
        api.deleteTable(name.catalogName, name.databaseName, name.tableName)
        database = api.getDatabase(name.catalogName, name.databaseName, false)

        then:
        !database.tables.contains(name.tableName)

        when:
        api.getTable(name.catalogName, name.databaseName, name.tableName, true, false, false)
        TestCatalogs.findByQualifiedName(name).createdTables.remove(name)

        then:
        thrown(MetacatNotFoundException)

        where:
        name << TestCatalogs.getCreatedTables(TestCatalogs.getCanDeleteTable(TestCatalogs.ALL))
    }

    def 'deleteDatabase: #name nonexistent fails'() {
        when:
        def catalog = api.getCatalog(name.catalogName)

        then:
        !catalog.databases.contains(name.databaseName)

        when:
        api.deleteDatabase(name.catalogName, name.databaseName)

        then:
        thrown(MetacatNotFoundException)

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.getCanDeleteDatabase(TestCatalogs.ALL))
            .collect { QualifiedName.ofDatabase(it.catalogName, 'does_not_exist') }
    }


    def 'deleteDatabase: can delete #name'() {
        when:
        def catalog = api.getCatalog(name.catalogName)

        then:
        catalog.databases.contains(name.databaseName)

        when:
        api.deleteDatabase(name.catalogName, name.databaseName)
        catalog = api.getCatalog(name.catalogName)

        then:
        !catalog.databases.contains(name.databaseName)

        when:
        api.getDatabase(name.catalogName, name.databaseName, false)
        TestCatalogs.findByQualifiedName(name).createdDatabases.remove(name)

        then:
        thrown(MetacatNotFoundException)

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.getCanDeleteDatabase(TestCatalogs.ALL))
    }
}
