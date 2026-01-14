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

import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.converter.DefaultTypeConverter
import com.netflix.metacat.common.server.converter.DozerJsonTypeConverter
import com.netflix.metacat.common.server.converter.DozerTypeConverter
import com.netflix.metacat.common.server.converter.TypeConverterFactory
import com.netflix.metacat.thrift.HiveConnectorInfoConverter
import com.netflix.metacat.thrift.converters.HiveTypeConverter
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Created by amajumdar on 5/12/15.
 */
class MetacatSmokeThriftSpec extends Specification {
    @Shared
    ConverterUtil converter
    @Shared
    HiveConnectorInfoConverter hiveConverter
    @Shared
    Hive remoteHiveClient

    static final String METADATA_LOCATION = 'file:/tmp/data/metadata/00000-0b60cc39-438f-413e-96c2-2694d7926529.metadata.json'

    def setupSpec() {
        Logger.getRootLogger().setLevel(Level.OFF)

        HiveConf conf = new HiveConf()
        conf.set('hive.metastore.uris', "thrift://localhost:${System.properties['metacat_hive_thrift_port']}")
        SessionState.setCurrentSessionState(new SessionState(conf))
        remoteHiveClient = Hive.get(conf)

        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger("ROOT")).setLevel(ch.qos.logback.classic.Level.OFF)
        def typeFactory = new TypeConverterFactory(new DefaultTypeConverter())
        converter = new ConverterUtil(new DozerTypeConverter(typeFactory), new DozerJsonTypeConverter(typeFactory))
        hiveConverter = new HiveConnectorInfoConverter(new HiveTypeConverter())
    }
    @Shared
    boolean isLocalEnv = Boolean.valueOf(System.getProperty("local", "true"))

    @Ignore
    def getUri(String databaseName, String tableName) {
        return isLocalEnv ? String.format('file:/tmp/%s/%s', databaseName, tableName) : String.format("s3://wh/%s.db/%s", databaseName, tableName)
    }

    @Ignore
    def getUpdatedUri(String databaseName, String tableName) {
        return isLocalEnv ? String.format('file:/tmp/%s/%s/%s', databaseName, tableName, 'updated') : String.format("s3://wh/%s.db/%s/%s", databaseName, tableName, 'updated')
    }

    @Unroll
    def "Test create database for catalog #catalogName"() {
        when:
        def databaseName = 'test_db1_' + catalogName
        client.createDatabase(new Database(databaseName, 'test_db1', null, null))
        then:
        def database = client.getDatabase(databaseName)
        assert database != null && database.name == 'test_db1_' + catalogName
        when:
        client.createDatabase(new Database(databaseName, 'test_db1', null, null))
        then:
        thrown(AlreadyExistsException)
        cleanup:
        client.dropDatabase(databaseName)
        where:
        catalogName | client
        'remote'    | remoteHiveClient
    }

    @Unroll
    def "Test update database for catalog #catalogName"() {
        given:
        def invalidDatabaseName = 'test_db1_invalid' + catalogName
        def databaseName = 'test_db1_' + catalogName
        def exceptionThrown = false
        client.createDatabase(new Database(databaseName, 'test_db1', null, null))
        when:
        client.alterDatabase(databaseName, new Database(databaseName, 'test_db1', null, null))
        then:
        def database = client.getDatabase(databaseName)
        def databaseUri = database.locationUri
        database != null && database.name == 'test_db1_' + catalogName
        when:
        client.alterDatabase(databaseName, new Database(databaseName, 'test_db1', databaseUri + 1, null))
        then:
        def uDatabase = client.getDatabase(databaseName)
        uDatabase != null && (catalogName != 'localfast' || uDatabase.name == 'test_db1_' + catalogName && uDatabase.locationUri == databaseUri + 1)
        when:
        try {
            client.alterDatabase(invalidDatabaseName, new Database(invalidDatabaseName, 'test_db1', null, null))
        } catch (Exception e) {
            exceptionThrown = true
        }
        then:
        (catalogName == 'localfast' && exceptionThrown)
            || (catalogName == 'local' && exceptionThrown)
            || (catalogName == 'remote' && exceptionThrown)
        cleanup:
        client.dropDatabase(databaseName)
        where:
        catalogName | client
        'remote'    | remoteHiveClient
    }

    @Unroll
    def "Test drop database for catalog #catalogName"() {
        given:
        def invalidDatabaseName = 'test_db1_invalid' + catalogName
        def databaseName = 'test_db1_' + catalogName
        client.createDatabase(new Database(databaseName, 'test_db1', null, null))
        when:
        client.dropDatabase(databaseName)
        then:
        client.getDatabase(databaseName) == null
        when:
        client.dropDatabase(invalidDatabaseName)
        then:
        thrown(NoSuchObjectException)
        where:
        catalogName | client
        'remote'    | remoteHiveClient
    }

    @Unroll
    def "Test create table"() {
        given:
        def databaseName = 'test_db2_' + catalogName
        def tableName = 'test_create_table'
        try {
            client.createDatabase(new Database(databaseName, 'test_db', null, null))
        } catch (Exception ignored) {
        }
        def uri = getUri(databaseName, tableName)
        // Set up table parameters with Iceberg metadata
        def tableParams = [
            'table_type': 'ICEBERG',
            'metadata_location': METADATA_LOCATION
        ]
        // Create Hive Table object
        def hiveTable = new Table(databaseName, tableName)
        hiveTable.owner = 'test_owner'
        hiveTable.setParamters(tableParams)
        hiveTable.sd.cols = [new FieldSchema('field1', 'string', 'field1 comment')]
        hiveTable.sd.location = uri
        hiveTable.sd.inputFormat = 'org.apache.hadoop.mapred.TextInputFormat'
        hiveTable.sd.outputFormat = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        hiveTable.sd.serdeInfo = new SerDeInfo(tableName, 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', ['serialization.format': '1'])
        when:
        client.createTable(hiveTable)
        then:
        def table = client.getTable(databaseName, tableName)
        table != null && table.getTableName() == tableName
        table.getParameters().get('metadata_location') == METADATA_LOCATION
        // Owner should match what was explicitly set in the create request
        table.getOwner() == 'test_owner'
        when:
        client.createTable(hiveTable)
        then:
        thrown(Exception.class)
        cleanup:
        client.dropTable(databaseName, tableName)
        where:
        catalogName | client
        'remote'    | remoteHiveClient
    }

    @Unroll
    def "Test rename table for #catalogName to test_create_table1"() {
        when:
        def databaseName = 'test_db3_' + catalogName
        def tableName = 'test_create_table'
        def newTableName = 'test_create_table1'
        try {
            client.createDatabase(new Database(databaseName, 'test_db', null, null))
        } catch (Exception ignored) {
        }
        def uri = getUri(databaseName, tableName)
        // Set up table parameters with Iceberg metadata
        def tableParams = [
            'table_type': 'ICEBERG',
            'metadata_location': METADATA_LOCATION
        ]
        // Create Hive Table object
        def hiveTable = new Table(databaseName, tableName)
        hiveTable.owner = 'test_owner'
        hiveTable.setParamters(tableParams)
        hiveTable.sd.cols = [new FieldSchema('field1', 'string', 'field1 comment')]
        hiveTable.sd.location = uri
        hiveTable.sd.inputFormat = 'org.apache.hadoop.mapred.TextInputFormat'
        hiveTable.sd.outputFormat = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        hiveTable.sd.serdeInfo = new SerDeInfo(tableName, 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', ['serialization.format': '1'])
        client.createTable(hiveTable)
        hiveTable.setTableName(newTableName)
        client.alterTable(databaseName + '.' + tableName, hiveTable)
        then:
        def table = client.getTable(databaseName, newTableName)
        table != null && table.getTableName() == newTableName
        when:
        hiveTable.setTableType(TableType.VIRTUAL_VIEW)
        hiveTable.setViewOriginalText('select 1')
        hiveTable.setViewExpandedText('select 1')
        client.alterTable(databaseName + '.' + newTableName, hiveTable)
        hiveTable.setTableName(tableName)
        client.alterTable(databaseName + '.' + newTableName, hiveTable)
        table = client.getTable(databaseName, tableName)
        then:
        table != null && table.getTableName() == tableName
        cleanup:
        client.dropTable(databaseName, tableName)
        where:
        catalogName | client
        'remote'    | remoteHiveClient
    }
}
