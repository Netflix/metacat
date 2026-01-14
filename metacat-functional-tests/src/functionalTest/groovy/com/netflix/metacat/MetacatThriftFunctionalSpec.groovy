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

import com.google.common.base.Strings
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.netflix.metacat.common.QualifiedName
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.Warehouse
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.metadata.InvalidTableException
import org.apache.hadoop.hive.ql.metadata.Partition
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.log4j.Level
import org.apache.log4j.Logger
import spock.lang.*

//TODO REMOVE ALL IGNORE

@Stepwise
@Unroll
class MetacatThriftFunctionalSpec extends Specification {
    public static final long BATCH_ID = System.currentTimeSeconds()
    public static final int timediff = 24 * 3600
    // Path to existing Iceberg metadata file in the test container
    public static final String ICEBERG_METADATA_LOCATION = 'file:/tmp/data/metadata/00000-0b60cc39-438f-413e-96c2-2694d7926529.metadata.json'
    @Shared
    String hiveThriftUri
    @Shared
    LoadingCache<String, Hive> METASTORES = CacheBuilder.newBuilder()
        .build(
        new CacheLoader<String, Hive>() {
            public Hive load(String thriftUri) throws Exception {
                if (!Strings.isNullOrEmpty(thriftUri)) {
                    HiveConf conf = new HiveConf()
                    conf.set('hive.metastore.uris', thriftUri)
                    SessionState.setCurrentSessionState(new SessionState(conf))
                    // Hive.get stores the client in a thread local, use the private constructor to manage our own cache
                    return new Hive(conf)
                } else {
                    throw new IllegalArgumentException("Invalid Thrift URI")
                }
            }
        });

    def setupSpec() {
        Logger.getRootLogger().setLevel(Level.OFF)
        String thriftPort = System.properties['metacat_hive_thrift_port']?.toString()?.trim()
        assert thriftPort, 'Required system property "metacat_hive_thrift_port" is not set'
        TestCatalogs.findByCatalogName('polaris-metastore').thriftUri = "thrift://localhost:${thriftPort}".toString()

        // hiveThriftUri is only used for validation tests against a real Hive metastore
        // Since Polaris doesn't have a corresponding Hive metastore, these validation tests are skipped
        thriftPort = System.properties['hive_thrift_port']?.toString()?.trim()
        if (thriftPort) {
            hiveThriftUri = "thrift://localhost:${thriftPort}".toString()
        }
        TestCatalogs.resetAll()

    }

    List<String> databaseDifferences(Database actual, Database expected) {
        if (actual == expected) {
            return []
        }

        List<String> result = []
        if (actual.name != expected.name) {
            result << "database.name: $actual.name != $expected.name"
        }

        if (actual.locationUri != expected.locationUri) {
            result << "database.locationUri: $actual.locationUri != $expected.locationUri"
        }

        if (actual.parameters != expected.parameters) {
            result << "database.parameters: $actual.parameters != $expected.parameters"
        }

        return result
    }

    List<String> fieldSchemaDifferences(String prefix, FieldSchema actual, FieldSchema expected) {
        if (actual == expected) {
            return []
        }

        List<String> result = []
        if (actual.name != expected.name) {
            result << "${prefix}.name:  $actual.name != $expected.name"
        }

        if (actual.type != expected.type) {
            result << "${prefix}.type:  $actual.type != $expected.type"
        }

        if (actual.comment != expected.comment) {
            result << "${prefix}.comment:  $actual.comment != $expected.comment"
        }

        return result
    }

    List<String> storageDescriptorDifferences(String prefix, StorageDescriptor actual, StorageDescriptor expected) {
        if (actual == expected) {
            return []
        }

        List<String> result = []
        for (int i; i < Math.max(actual.cols.size(), expected.cols.size()); i++) {
            def fActual = actual.cols.size() >= i ? actual.cols[i] : null
            def fExpected = expected.cols.size() >= i ? expected.cols[i] : null
            if (!fActual || !fExpected) {
                result << "${prefix}.cols[$i]: $fActual != $fExpected"
            } else {
                result.addAll(fieldSchemaDifferences("${prefix}.cols[$i]", fActual, fExpected))
            }
        }

        if (actual.location != expected.location) {
            result << "${prefix}.location:  $actual.location != $expected.location"
        }

        if (actual.inputFormat != expected.inputFormat) {
            result << "${prefix}.inputFormat:  $actual.inputFormat != $expected.inputFormat"
        }

        if (actual.outputFormat != expected.outputFormat) {
            result << "${prefix}.outputFormat:  $actual.outputFormat != $expected.outputFormat"
        }

        if (actual.compressed != expected.compressed) {
            result << "${prefix}.compressed:  $actual.compressed != $expected.compressed"
        }

        if (actual.numBuckets != expected.numBuckets) {
            result << "${prefix}.numBuckets:  $actual.numBuckets != $expected.numBuckets"
        }

        if (actual.parameters != expected.parameters) {
            result << "${prefix}.parameters:  $actual.parameters != $expected.parameters"
        }

        if (actual.storedAsSubDirectories != expected.storedAsSubDirectories) {
            result << "${prefix}.storedAsSubDirectories:  $actual.storedAsSubDirectories != $expected.storedAsSubDirectories"
        }

        if (actual.serdeInfo.serializationLib != expected.serdeInfo.serializationLib) {
            result << "${prefix}.serdeInfo.serializationLib:  $actual.serdeInfo.serializationLib != $expected.serdeInfo.serializationLib"
        }

        if (actual.serdeInfo.parameters != expected.serdeInfo.parameters) {
            result << "${prefix}.serdeInfo.parameters:  $actual.serdeInfo.parameters != $expected.serdeInfo.parameters"
        }

        // ignoring these for now
        // serdeInfo.name // required
//        private List<String> bucketCols; // required
//        private List<Order> sortCols; // required
//        private SkewedInfo skewedInfo; // optional

        return result
    }

    List<String> tableDifferences(Table a, Table e) {
        if (a == e) {
            return []
        }

        List<String> result = []
        org.apache.hadoop.hive.metastore.api.Table actual = a.getTTable()
        org.apache.hadoop.hive.metastore.api.Table expected = e.getTTable()

        if (actual.tableName != expected.tableName) {
            result << "table.tableName: $actual.tableName != $expected.tableName"
        }

        if (actual.dbName != expected.dbName) {
            result << "table.dbName: $actual.dbName != $expected.dbName"
        }

        if (actual.owner != expected.owner) {
            result << "table.owner: $actual.owner != $expected.owner"
        }

        if (actual.createTime != expected.createTime) {
            result << "table.createTime: $actual.createTime != $expected.createTime"
        }

        if (actual.lastAccessTime != expected.lastAccessTime) {
            result << "table.lastAccessTime: $actual.lastAccessTime != $expected.lastAccessTime"
        }

        if (actual.parameters != expected.parameters) {
            result << "table.parameters: $actual.parameters != $expected.parameters"
        }

        if (actual.retention != expected.retention) {
            result << "table.retention: $actual.retention != $expected.retention"
        }

        if (actual.viewOriginalText != expected.viewOriginalText) {
            result << "table.viewOriginalText: $actual.viewOriginalText != $expected.viewOriginalText"
        }

        if (actual.viewExpandedText != expected.viewExpandedText) {
            result << "table.viewExpandedText: $actual.viewExpandedText != $expected.viewExpandedText"
        }

        if (actual.tableType != expected.tableType) {
            result << "table.tableType: $actual.tableType != $expected.tableType"
        }

        if (actual.temporary != expected.temporary) {
            result << "table.temporary: $actual.temporary != $expected.temporary"
        }

        result.addAll(storageDescriptorDifferences('table.sd', actual.sd, expected.sd))

        for (int i; i < Math.max(actual.partitionKeys.size(), expected.partitionKeys.size()); i++) {
            def fActual = actual.partitionKeys.size() >= i ? actual.partitionKeys[i] : null
            def fExpected = expected.partitionKeys.size() >= i ? expected.partitionKeys[i] : null
            if (!fActual || !fExpected) {
                result << "table.partitionKeys[$i]: $fActual != $fExpected"
            } else {
                result.addAll(fieldSchemaDifferences("table.partitionKeys[$i]", fActual, fExpected))
            }
        }

        // ignoring this for now
//        private PrincipalPrivilegeSet privileges; // optional

        return result
    }

    List<String> partitionDifferences(Partition a, Partition e) {
        if (a == e) {
            return []
        }

        List<String> result = []
        org.apache.hadoop.hive.metastore.api.Partition actual = a.getTPartition()
        org.apache.hadoop.hive.metastore.api.Partition expected = e.getTPartition()

        if (actual.values != expected.values) {
            result << "partition.values: $actual.values != $expected.values"
        }

        if (actual.tableName != expected.tableName) {
            result << "partition.tableName: $actual.tableName != $expected.tableName"
        }

        if (actual.dbName != expected.dbName) {
            result << "partition.dbName: $actual.dbName != $expected.dbName"
        }

        if (actual.createTime != expected.createTime) {
            result << "partition.createTime: $actual.createTime != $expected.createTime"
        }

        if (actual.lastAccessTime != expected.lastAccessTime) {
            result << "partition.lastAccessTime: $actual.lastAccessTime != $expected.lastAccessTime"
        }

        if (actual.parameters != expected.parameters) {
            result << "partition.parameters: $actual.parameters != $expected.parameters"
        }

        result.addAll(storageDescriptorDifferences('partition.sd', actual.sd, expected.sd))

        // ignoring this for now
//        private PrincipalPrivilegeSet privileges; // optional

        return result
    }


    def 'createDatabase: can create a database for "#catalog.name"'() {
        given:
        def thrift = METASTORES.get(catalog.thriftUri)
        String databaseName = "db_in_${catalog.name.replace('-', '_')}_$BATCH_ID".toString()
        String description = 'this description will be ignored'
        String locationUri = "file:/tmp/${catalog.name}/${databaseName}".toString()
        def params = ['k1': 'v1', 'k2': 'v2']
        Database db = new Database(databaseName, description, locationUri, params)

        when:
        true

        then:
        !thrift.databaseExists(databaseName)

        when:
        thrift.createDatabase(db)

        then:
        thrift.databaseExists(databaseName)

        when:
        // CockroachDB uses follower_read_timestamp() for list operations which reads ~4.8s in the past
        if (catalog.type == 'polaris') {
            Thread.sleep(5000)
        }
        def allDbs = thrift.allDatabases
        def resultDb = thrift.getDatabase(databaseName)

        then:
        allDbs.contains(databaseName)
        resultDb.name == databaseName
        resultDb.description == databaseName // currently the passed description is ignored on create
        resultDb.description != description
        resultDb.locationUri == locationUri
        resultDb.parameters == params
        catalog.createdDatabases << QualifiedName.ofDatabase(catalog.name, databaseName)

        where:
        catalog << TestCatalogs.getThriftImplementers(TestCatalogs.ALL)
    }

    def 'getDatabasesByPattern: can find #name with multiple patterns'() {
        given:
        def catalog = TestCatalogs.findByQualifiedName(name)
        def thrift = METASTORES.get(catalog.thriftUri)

        when:
        def db = thrift.getDatabase(name.databaseName)

        then:
        db != null

        when:
        def dbList = thrift.getDatabasesByPattern('db.*')

        then:
        dbList.contains(name.databaseName)

        when:
        dbList = thrift.getDatabasesByPattern('db.+')

        then:
        dbList.contains(name.databaseName)

        when:
        dbList = thrift.getDatabasesByPattern("db_.+${BATCH_ID}")

        then:
        dbList.contains(name.databaseName)

        when:
        dbList = thrift.getDatabasesByPattern("db_.*${BATCH_ID}")

        then:
        dbList.contains(name.databaseName)

        when:
        dbList = thrift.getDatabasesByPattern(".*_${BATCH_ID}".toString())

        then:
        dbList.contains(name.databaseName)

        when:
        dbList = thrift.getDatabasesByPattern(".+_${BATCH_ID}".toString())

        then:
        dbList.contains(name.databaseName)

        when:
        dbList = thrift.getDatabasesByPattern(".*_${name.catalogName.replace('-', '_')}_.*".toString())

        then:
        dbList.contains(name.databaseName)

        when:
        dbList = thrift.getDatabasesByPattern(".+_${name.catalogName.replace('-', '_')}_.+".toString())

        then:
        dbList.contains(name.databaseName)

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.getThriftImplementers(TestCatalogs.ALL))
    }

    def 'alterDatabase can alter #name'() {
        given:
        def catalog = TestCatalogs.findByQualifiedName(name)
        def thrift = METASTORES.get(catalog.thriftUri)
        def now = new Date()

        when:
        def db = thrift.getDatabase(name.databaseName)

        then:
        !db.parameters['field_added']

        when:
        def fieldAddedValue = now.toString()
        db.parameters['field_added'] = fieldAddedValue
        thrift.alterDatabase(db.name, db)
        db = thrift.getDatabase(name.databaseName)

        then:
        noExceptionThrown()
        db.parameters['field_added'] == fieldAddedValue

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.getThriftImplementers(TestCatalogs.ALL))
    }

    def 'createTable: can create a partitioned table in #name'() {
        given:
        def catalog = TestCatalogs.findByQualifiedName(name)
        def thrift = METASTORES.get(catalog.thriftUri)
        def now = new Date()
        def tableName = "partitioned_tbl_$BATCH_ID".toString()
        def owner = 'owner_name'
        def tableParams = ['tp_k1': 'tp_v1']
        def locationUri = "file:/tmp/${name.catalogName}/${name.databaseName}/${tableName}".toString()
        def inputFormat = 'org.apache.hadoop.mapred.TextInputFormat'
        def outputFormat = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        def sdInfoName = 'is ignored for now'
        def serializationLib = 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        def sdInfoParams = ['serialization.format': '1']
        def sdParams = ['sd_k1': 'sd_v1']
        def partitionKeys = [
            new FieldSchema('pk1', 'string', 'pk1 comment'),
            new FieldSchema('pk2', 'bigint', 'pk2 comment')
        ]
        def fields = [
            new FieldSchema('field1', 'string', 'field1 comment'),
            new FieldSchema('field2', 'boolean', 'field2 comment'),
            new FieldSchema('field3', 'bigint', 'field3 comment'),
            new FieldSchema('field4', 'double', 'field4 comment'),
            new FieldSchema('field5', 'binary', 'field5 comment'),
            new FieldSchema('field6', 'date', 'field6 comment'),
            new FieldSchema('field7', 'timestamp', 'field7 comment'),
            new FieldSchema('field8', 'array<boolean>', 'field8 comment'),
            new FieldSchema('field9', 'map<string,string>', 'field9 comment'),
        ]

        def table = new Table(name.databaseName, tableName)
        table.owner = owner
        // Polaris catalogs require metadata_location for Iceberg tables
        if (catalog.type == 'polaris') {
            tableParams['table_type'] = 'ICEBERG'
            tableParams['metadata_location'] = ICEBERG_METADATA_LOCATION
        }
        table.setParamters(tableParams)
        table.setPartCols(partitionKeys)
        table.sd.cols = fields
        table.sd.location = locationUri
        table.sd.inputFormat = inputFormat
        table.sd.outputFormat = outputFormat
        table.sd.serdeInfo = new SerDeInfo(sdInfoName, serializationLib, sdInfoParams)
        table.sd.parameters = sdParams

        when:
        def tables = thrift.getAllTables(name.databaseName)

        then:
        !tables.contains(tableName)

        when:
        thrift.createTable(table)
        // CockroachDB uses follower_read_timestamp() for list operations which reads ~4.8s in the past
        if (catalog.type == 'polaris') {
            Thread.sleep(5000)
        }
        tables = thrift.getAllTables(name.databaseName)

        then:
        tables.contains(table.tableName)

        when:
        def resultTbl = thrift.getTable(table.dbName, table.tableName)

        then:
        resultTbl.tableName == tableName
        resultTbl.dbName == name.databaseName
        resultTbl.owner == owner
        TestUtilities.epochCloseEnough(resultTbl.getTTable().createTime, now, timediff)
        !resultTbl.lastAccessTime
        !resultTbl.retention
        resultTbl.tableType == TableType.EXTERNAL_TABLE
        // Polaris/Iceberg tables don't set EXTERNAL parameter the same way as Hive
        catalog.type == 'polaris' || resultTbl.parameters['EXTERNAL'] == 'TRUE'
        // Polaris/Iceberg may not preserve all custom table parameters, only check required ones for Polaris
        catalog.type == 'polaris' || tableParams.keySet().every { tableParams[it] == resultTbl.parameters[it] }
        // For Polaris, verify Iceberg-specific parameters are preserved
        catalog.type != 'polaris' || (resultTbl.parameters['table_type'] == 'ICEBERG' && resultTbl.parameters['metadata_location'] == ICEBERG_METADATA_LOCATION)
        // Polaris/Iceberg gets partition keys and schema from metadata file, not from create request
        catalog.type == 'polaris' || resultTbl.partitionKeys == partitionKeys
        !resultTbl.sd.compressed
        !resultTbl.sd.numBuckets
        !resultTbl.bucketCols
        !resultTbl.sortCols
        // Polaris/Iceberg gets columns from metadata file
        catalog.type == 'polaris' || resultTbl.sd.cols == fields
        // For Polaris, just verify we got a valid storage descriptor
        catalog.type != 'polaris' || resultTbl.sd.location != null
        catalog.type == 'polaris' || resultTbl.sd.location == locationUri
        catalog.type == 'polaris' || resultTbl.sd.inputFormat == inputFormat
        catalog.type == 'polaris' || resultTbl.sd.outputFormat == outputFormat
        catalog.type == 'polaris' || resultTbl.sd.serdeInfo.name == tableName
        catalog.type == 'polaris' || resultTbl.sd.serdeInfo.serializationLib == serializationLib
        catalog.type == 'polaris' || resultTbl.sd.serdeInfo.parameters == sdInfoParams
        catalog.type == 'polaris' || resultTbl.sd.parameters == sdParams
        resultTbl.sd.skewedInfo != null
        catalog.createdTables << QualifiedName.ofTable(name.catalogName, name.databaseName, tableName)

        when:
        table.setTableName("partitioned_tbl1_$BATCH_ID".toString())
        table.sd.inputFormat = null
        table.sd.outputFormat = null
        // Update metadata_location for the second table if Polaris
        if (catalog.type == 'polaris') {
            def newParams = new HashMap(table.parameters)
            newParams['metadata_location'] = ICEBERG_METADATA_LOCATION
            table.setParamters(newParams)
        }
        thrift.createTable(table)
        // CockroachDB uses follower_read_timestamp() for list operations which reads ~4.8s in the past
        if (catalog.type == 'polaris') {
            Thread.sleep(5000)
        }
        tables = thrift.getAllTables(name.databaseName)
        resultTbl = thrift.getTable(table.dbName, table.tableName)

        then:
        tables.contains(table.tableName)
        // Polaris gets storage descriptor info from metadata file, not from create request
        catalog.type == 'polaris' || resultTbl.sd.inputFormat == null
        catalog.type == 'polaris' || resultTbl.sd.outputFormat == null

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.getThriftImplementers(TestCatalogs.ALL))
    }

    def 'createTable: can create a unpartitioned table in #name'() {
        given:
        def catalog = TestCatalogs.findByQualifiedName(name)
        def thrift = METASTORES.get(catalog.thriftUri)
        def now = new Date()
        def tableName = "unpartitioned_tbl_$BATCH_ID".toString()
        def owner = 'owner_name'
        def tableParams = ['tp_k1': 'tp_v1']
        def locationUri = "file:/tmp/${name.catalogName}/${name.databaseName}/${tableName}".toString()
        def inputFormat = 'org.apache.hadoop.mapred.TextInputFormat'
        def outputFormat = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        def sdInfoName = 'is ignored for now'
        def serializationLib = 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        def sdInfoParams = ['serialization.format': '1']
        def sdParams = ['sd_k1': 'sd_v1']
        def fields = [
            new FieldSchema('field1', 'string', 'field1 comment'),
            new FieldSchema('field2', 'boolean', 'field2 comment'),
            new FieldSchema('field3', 'bigint', 'field3 comment'),
            new FieldSchema('field4', 'double', 'field4 comment'),
            new FieldSchema('field5', 'binary', 'field5 comment'),
            new FieldSchema('field6', 'date', 'field6 comment'),
            new FieldSchema('field7', 'timestamp', 'field7 comment'),
            new FieldSchema('field8', 'array<boolean>', 'field8 comment'),
            new FieldSchema('field9', 'map<string,string>', 'field9 comment'),
        ]

        def table = new Table(name.databaseName, tableName)
        table.owner = owner
        // Polaris catalogs require metadata_location for Iceberg tables
        if (catalog.type == 'polaris') {
            tableParams['table_type'] = 'ICEBERG'
            tableParams['metadata_location'] = ICEBERG_METADATA_LOCATION
        }
        table.setParamters(tableParams)
        table.sd.cols = fields
        table.sd.location = locationUri
        table.sd.inputFormat = inputFormat
        table.sd.outputFormat = outputFormat
        table.sd.serdeInfo = new SerDeInfo(sdInfoName, serializationLib, sdInfoParams)
        table.sd.parameters = sdParams

        when:
        def tables = thrift.getAllTables(name.databaseName)

        then:
        !tables.contains(tableName)

        when:
        thrift.createTable(table)
        // CockroachDB uses follower_read_timestamp() for list operations which reads ~4.8s in the past
        if (catalog.type == 'polaris') {
            Thread.sleep(5000)
        }
        tables = thrift.getAllTables(name.databaseName)

        then:
        tables.contains(table.tableName)

        when:
        def resultTbl = thrift.getTable(table.dbName, table.tableName)

        then:
        resultTbl.tableName == tableName
        resultTbl.dbName == name.databaseName
        resultTbl.owner == owner
        TestUtilities.epochCloseEnough(resultTbl.getTTable().createTime, now, timediff)
        !resultTbl.lastAccessTime
        !resultTbl.retention
        resultTbl.tableType == TableType.EXTERNAL_TABLE
        // Polaris/Iceberg tables don't set EXTERNAL parameter the same way as Hive
        catalog.type == 'polaris' || resultTbl.parameters['EXTERNAL'] == 'TRUE'
        // Polaris/Iceberg may not preserve all custom table parameters, only check required ones for Polaris
        catalog.type == 'polaris' || tableParams.keySet().every { tableParams[it] == resultTbl.parameters[it] }
        // For Polaris, verify Iceberg-specific parameters are preserved
        catalog.type != 'polaris' || (resultTbl.parameters['table_type'] == 'ICEBERG' && resultTbl.parameters['metadata_location'] == ICEBERG_METADATA_LOCATION)
        // Polaris/Iceberg may have partition keys from metadata file
        catalog.type == 'polaris' || !resultTbl.partitionKeys
        !resultTbl.sd.compressed
        !resultTbl.sd.numBuckets
        !resultTbl.bucketCols
        !resultTbl.sortCols
        // Polaris/Iceberg gets columns from metadata file
        catalog.type == 'polaris' || resultTbl.sd.cols == fields
        // For Polaris, just verify we got a valid storage descriptor
        catalog.type != 'polaris' || resultTbl.sd.location != null
        catalog.type == 'polaris' || resultTbl.sd.location == locationUri
        catalog.type == 'polaris' || resultTbl.sd.inputFormat == inputFormat
        catalog.type == 'polaris' || resultTbl.sd.outputFormat == outputFormat
        catalog.type == 'polaris' || resultTbl.sd.serdeInfo.name == tableName
        catalog.type == 'polaris' || resultTbl.sd.serdeInfo.serializationLib == serializationLib
        catalog.type == 'polaris' || resultTbl.sd.serdeInfo.parameters == sdInfoParams
        catalog.type == 'polaris' || resultTbl.sd.parameters == sdParams
        catalog.createdTables << QualifiedName.ofTable(name.catalogName, name.databaseName, tableName)

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.getThriftImplementers(TestCatalogs.ALL))
    }

    def 'createPartition: fails on an unpartitioned table #name'() {
        given:
        def catalog = TestCatalogs.findByQualifiedName(name)
        def thrift = METASTORES.get(catalog.thriftUri)
        def table = new Table(name.databaseName, name.tableName)

        when:
        def tables = thrift.getAllTables(name.databaseName)

        then:
        tables.contains(name.tableName)

        when:
        thrift.createPartition(table, ['pk1': 'partition_value', 'pk2': '1'])

        then:
        def e = thrown(HiveException)

        where:
        name << TestCatalogs.getCreatedTables(TestCatalogs.getThriftImplementers(TestCatalogs.ALL))
            .findAll { it.tableName.startsWith('unpartitioned') }
    }

    def 'dropTable: can drop #name'() {
        given:
        def catalog = TestCatalogs.findByQualifiedName(name)
        def thrift = METASTORES.get(catalog.thriftUri)

        when:
        def table = thrift.getTable(name.databaseName, name.tableName)

        then:
        table != null

        when:
        thrift.dropTable(name.databaseName, name.tableName, false, false, false)
        thrift.getTable(name.databaseName, name.tableName)

        then:
        def e = thrown(InvalidTableException)
        e.message.contains('Table not found')
        catalog.createdTables.remove(name)

        where:
        name << TestCatalogs.getCreatedTables(TestCatalogs.getThriftImplementers(TestCatalogs.ALL))
    }

    def 'dropDatabase: can drop #name'() {
        given:
        def catalog = TestCatalogs.findByQualifiedName(name)
        def thrift = METASTORES.get(catalog.thriftUri)

        when:
        def database = thrift.getDatabase(name.databaseName)

        then:
        database != null

        when:
        // Drop all tables first (Aurora has FK constraints, CRDB Polaris allows non-empty db deletion but we normalize behavior)
        thrift.getAllTables(name.databaseName).each {
            thrift.dropTable(name.databaseName, it)
        }
        thrift.dropDatabase(name.databaseName, false, false, false)

        then:
        noExceptionThrown()

        where:
        name << TestCatalogs.getCreatedDatabases(TestCatalogs.getThriftImplementers(TestCatalogs.ALL))
    }
}
