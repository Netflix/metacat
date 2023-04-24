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

import com.google.common.collect.Lists
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.converter.ConverterUtil
import com.netflix.metacat.common.server.converter.DefaultTypeConverter
import com.netflix.metacat.common.server.converter.DozerJsonTypeConverter
import com.netflix.metacat.common.server.converter.DozerTypeConverter
import com.netflix.metacat.common.server.converter.TypeConverterFactory
import com.netflix.metacat.common.server.partition.util.PartitionUtil
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.testdata.provider.DataDtoProvider
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.Partition
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan.DropTableDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.thrift.TApplicationException
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
    Hive localHiveClient
    @Shared
    Hive localFastHiveClient
    @Shared
    Hive remoteHiveClient

    def setupSpec() {
        Logger.getRootLogger().setLevel(Level.OFF)

        HiveConf conf = new HiveConf()
        conf.set('hive.metastore.uris', "thrift://localhost:${System.properties['metacat_hive_thrift_port']}")
        SessionState.setCurrentSessionState(new SessionState(conf))
        remoteHiveClient = Hive.get(conf)
        // clients.add(Pair.of('remote', Hive.get(conf)))

        HiveConf localConf = new HiveConf()
        localConf.set('hive.metastore.uris', "thrift://localhost:${System.properties['metacat_embedded_hive_thrift_port']}")
        SessionState.setCurrentSessionState(new SessionState(localConf))
        localHiveClient = Hive.get(localConf)
        // clients.add(Pair.of('local', Hive.get(localConf)))

        HiveConf localFastConf = new HiveConf()
        localFastConf.set('hive.metastore.uris', "thrift://localhost:${System.properties['metacat_embedded_fast_hive_thrift_port']}")
        SessionState.setCurrentSessionState(new SessionState(localFastConf))
        localFastHiveClient = Hive.get(localFastConf)
        // clients.add(Pair.of('localfast', Hive.get(localFastConf)))

        ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger("ROOT")).setLevel(ch.qos.logback.classic.Level.OFF)
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

    @Ignore
    def createTable(Hive client, String catalogName, String databaseName, String tableName) {
        try {
            client.createDatabase(new Database(databaseName, 'test_db', null, null))
        } catch (Exception ignored) {
        }
        def hiveTable = client.getTable(databaseName, tableName, false)
        def owner = 'test_owner'
        def uri = null
        if (Boolean.valueOf(System.getProperty("local", "true"))) {
            uri = String.format('file:/tmp/%s/%s', databaseName, tableName)
        }
        if (hiveTable == null) {
            def newTable;
            if ('part' == tableName) {
                newTable = DataDtoProvider.getPartTable(catalogName, databaseName, owner, uri)
            } else if ('parts' == tableName) {
                newTable = DataDtoProvider.getPartsTable(catalogName, databaseName, owner, uri)
            } else {
                newTable = DataDtoProvider.getTable(catalogName, databaseName, tableName, owner, uri)
            }
            hiveTable = new Table(hiveConverter.fromTableInfo(converter.fromTableDto(newTable)))
            client.createTable(hiveTable)
        }
        return hiveTable
    }

    @Unroll
    def "Test create tables for catalog #catalogName"() {
        when:
        def databaseName = 'test_db_' + catalogName
        createTable(client, catalogName, databaseName, 'part')
        createTable(client, catalogName, databaseName, 'parts')
        then:
        noExceptionThrown()
        cleanup:
        client.dropTable(databaseName, 'part')
        client.dropTable(databaseName, 'parts')
        where:
        catalogName | client
        'remote'    | remoteHiveClient
        'local'     | localHiveClient
        'localfast' | localFastHiveClient
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
        'local'     | localHiveClient
        'localfast' | localFastHiveClient
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
            e.printStackTrace()
            exceptionThrown = true
        }
        then:
        (catalogName == 'localfast' && exceptionThrown)
            || (catalogName == 'local' && exceptionThrown)
            || (catalogName == 'remote' && !exceptionThrown)
        cleanup:
        client.dropDatabase(databaseName)
        where:
        catalogName | client
        'local'     | localHiveClient
        'localfast' | localFastHiveClient
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
        'local'     | localHiveClient
        'localfast' | localFastHiveClient
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
        def dto = DataDtoProvider.getTable(catalogName, databaseName, tableName, null, uri)
        def hiveTable = new Table(hiveConverter.fromTableInfo(converter.fromTableDto(dto)))
        when:
        client.createTable(hiveTable)
        then:
        def table = client.getTable(databaseName, tableName)
        table != null && table.getTableName() == tableName
        table.getSd().getLocation() == uri
        table.getOwner() == client.getConf().getUser()
        when:
        client.createTable(hiveTable)
        then:
        thrown(Exception.class)
        cleanup:
        client.dropTable(databaseName, tableName)
        where:
        catalogName | client
        'remote'    | remoteHiveClient
        'local'     | localHiveClient
        'localfast' | localFastHiveClient
    }

    @Unroll
    def "Test rename table for #catalogName to test_create_table1"() {
        when:
        def databaseName = 'test_db3_' + catalogName
        def tableName = 'test_create_table'
        def newTableName = 'test_create_table1'
        def hiveTable = createTable(client, catalogName, databaseName, tableName)
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
        'local'     | localHiveClient
        'localfast' | localFastHiveClient
    }

    @Unroll
    def "Test save partitions"() {
        given:
        def uri = isLocalEnv ? 'file:/tmp/abc' : null;
        def databaseName = 'test_db4_' + catalogName
        def tableName = 'part'
        def partitionName = 'one=xyz'
        def hiveTable = createTable(client, catalogName, databaseName, tableName)
        def dto = converter.toTableDto(hiveConverter.toTableInfo(QualifiedName.ofTable(catalogName, databaseName, tableName), hiveTable.getTTable()))
        def partitionDto = DataDtoProvider.getPartition(catalogName, databaseName, tableName, partitionName, uri)
        def partition = new Partition(hiveTable, hiveConverter.fromPartitionInfo(converter.fromTableDto(dto), converter.fromPartitionDto(partitionDto)))
        when:
        client.alterPartition(databaseName, tableName, partition)
        def partitions = client.getPartitionsByFilter(hiveTable, partitionName.replace("=", "='") + "'")
        def partitionss = client.getPartitionsByNames(hiveTable, [partitionName])
        then:
        partitions != null && partitions.size() == 1 && partitions.get(0).name == partitionName
        partitionss != null && partitionss.size() == 1 && partitionss.get(0).name == partitionName
        when:
        client.alterPartition(databaseName, tableName, partition)
        client.alterPartition(databaseName, tableName, partition)
        def rPartitions = client.getPartitionsByFilter(hiveTable, partitionName.replace("=", "='") + "'")
        def rPartitionss = client.getPartitionsByNames(hiveTable, [partitionName])
        then:
        rPartitions != null && rPartitions.size() == 1 && rPartitions.get(0).name == partitionName
        rPartitionss != null && rPartitionss.size() == 1 && rPartitionss.get(0).name == partitionName
        cleanup:
        client.dropPartition(databaseName, tableName, Lists.newArrayList(PartitionUtil.getPartitionKeyValues(partitionName).values()), false)
        where:
        catalogName | client
        'remote'    | remoteHiveClient
        'local'     | localHiveClient
        'localfast' | localFastHiveClient
    }

    @Unroll
    def "Test: Remote Thrift connector: drop partitions"() {
        given:
        def catalogName = 'remote'
        def client = remoteHiveClient

        def databaseName = 'test_db5_' + catalogName
        def tableName = 'parts'
        def hiveTable = createTable(client, catalogName, databaseName, tableName)
        def uri = isLocalEnv ? 'file:/tmp/abc' : null;
        def dto = converter.toTableDto(hiveConverter.toTableInfo(QualifiedName.ofTable(catalogName, databaseName, tableName), hiveTable.getTTable()))
        def partitionDtos = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, 'one=xyz/total=1', uri, 10)
        def partitions = partitionDtos.collect {
            new Partition(hiveTable, hiveConverter.fromPartitionInfo(converter.fromTableDto(dto), converter.fromPartitionDto(it)))
        }
        def oneColType = TypeInfoFactory.getPrimitiveTypeInfo('string')
        def totalColType = TypeInfoFactory.getPrimitiveTypeInfo('int')
        def oneColExpr = new ExprNodeColumnDesc(oneColType, 'one', null, true)
        def totalColExpr = new ExprNodeColumnDesc(totalColType, 'total', null, true)
        client.alterPartitions(databaseName + '.' + tableName, partitions)
        def partitionNames = client.getPartitionNames(databaseName, tableName, (short) -1)
        when:
        client.dropPartition(databaseName, tableName, Lists.newArrayList(PartitionUtil.getPartitionKeyValues(partitionNames[0]).values()), false)
        then:
        client.getPartitionsByNames(hiveTable, [partitionNames[0]]).size() == 0
        when:
        def totalExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getFunctionInfo('=').getGenericUDF(), Lists.newArrayList(totalColExpr, new ExprNodeConstantDesc(totalColType, 11)))
        def totalExpr1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getFunctionInfo('=').getGenericUDF(), Lists.newArrayList(totalColExpr, new ExprNodeConstantDesc(totalColType, 12)))
        client.dropPartitions(databaseName, tableName, [new DropTableDesc.PartSpec(totalExpr, 0), new DropTableDesc.PartSpec(totalExpr1, 0)], false, false, false)
        then:
        client.getPartitionsByNames(hiveTable, [partitionNames[1],partitionNames[2]]).size() == 0
        when:
        def oneExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getFunctionInfo('=').getGenericUDF(), Lists.newArrayList(oneColExpr, new ExprNodeConstantDesc(oneColType, 'xyz')))
        client.dropPartitions(databaseName, tableName, [new DropTableDesc.PartSpec(oneExpr, 0)], false, false, false)
        then:
        client.getPartitionNames(databaseName, tableName, (short) -1).size() == 0
    }

    @Unroll
    def "Test: Remote Thrift connector: get partitions for filter #filter returned #result partitions"() {
        when:
        def catalogName = 'remote'
        def client = remoteHiveClient

        def databaseName = 'test_db5_' + catalogName
        def tableName = 'parts'
        def hiveTable = createTable(client, catalogName, databaseName, tableName)
        if (cursor == 'start') {
            def uri = isLocalEnv ? 'file:/tmp/abc' : null;
            def dto = converter.toTableDto(hiveConverter.toTableInfo(QualifiedName.ofTable(catalogName, databaseName, tableName), hiveTable.getTTable()))
            def partitionDtos = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, 'one=xyz/total=1', uri, 10)
            def partitions = partitionDtos.collect {
                new Partition(hiveTable, hiveConverter.fromPartitionInfo(converter.fromTableDto(dto), converter.fromPartitionDto(it)))
            }
            client.alterPartitions(databaseName + '.' + tableName, partitions)
        }
        def resultPartitions = client.getPartitionsByFilter(hiveTable, filter)
        def firstPartition = resultPartitions.isEmpty() ? null : resultPartitions.get(0)
        then:
        resultPartitions.size() == (filter.contains('like') ? 0 : result)
        firstPartition == null || firstPartition.getParameters().size() > 1
        cleanup:
        if (cursor == 'end') {
            def partitionNames = client.getPartitionNames(databaseName, tableName, (short) -1)
            partitionNames.each {
                client.dropPartition(databaseName, tableName, Lists.newArrayList(PartitionUtil.getPartitionKeyValues(it).values()), false)
            }
        }
        where:
        cursor  | filter                                 | result
        'start' | "one='xyz'"                            | 10
        ''      | "one='xyz' and one like 'xy_'"         | 0
        ''      | "one like 'xy%'"                       | 10
        ''      | "total=10"                             | 1
        ''      | "total<1"                              | 0
        ''      | "total>1"                              | 10
        ''      | "total>=10"                            | 10
        ''      | "total<=20"                            | 10
        ''      | "total between 1 and 20"               | 10
        ''      | "total not between 1 and 20"           | 0
        'end'   | "one='xyz' and (total=11 or total=12)" | 2
    }

    @Unroll
    def "Test: Remote Thrift connector: getPartitions methods"() {
        when:
        def catalogName = 'remote'
        def client = remoteHiveClient

        def databaseName = 'test_db5_' + catalogName
        def tableName = 'parts'
        def hiveTable = createTable(client, catalogName, databaseName, tableName)
        def uri = isLocalEnv ? 'file:/tmp/abc' : null;
        def dto = converter.toTableDto(hiveConverter.toTableInfo(QualifiedName.ofTable(catalogName, databaseName, tableName), hiveTable.getTTable()))
        def partitionDtos = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, 'one=xyz/total=1', uri, 10)
        def partitions = partitionDtos.collect {
            new Partition(hiveTable, hiveConverter.fromPartitionInfo(converter.fromTableDto(dto), converter.fromPartitionDto(it)))
        }
        def oneColType = TypeInfoFactory.getPrimitiveTypeInfo('string')
        def totalColType = TypeInfoFactory.getPrimitiveTypeInfo('int')
        def oneColExpr = new ExprNodeColumnDesc(oneColType, 'one', null, true)
        def totalColExpr = new ExprNodeColumnDesc(totalColType, 'total', null, true)
        def oneExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getFunctionInfo('=').getGenericUDF(), Lists.newArrayList(oneColExpr, new ExprNodeConstantDesc(oneColType, 'xyz')))
        def totalExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getFunctionInfo('=').getGenericUDF(), Lists.newArrayList(totalColExpr, new ExprNodeConstantDesc(totalColType, 11)))
        client.alterPartitions(databaseName + '.' + tableName, partitions)
        then:
        client.getPartitions(hiveTable, ['one':'xyz']).size() == 10
        client.getPartitions(hiveTable, ['one':'xyz1']).size() == 0
        client.getPartitions(hiveTable, ['one':'xyz','total':'11']).size() == 1
        client.getPartitions(hiveTable, ['one':'xyz1','total':'1']).size() == 0
        when:
        def result = []
        client.getPartitionsByExpr(hiveTable, oneExpr, client.getConf(), result)
        def result1 = []
        client.getPartitionsByExpr(hiveTable, totalExpr, client.getConf(), result1)
        def oneExprInvalid = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getFunctionInfo('=').getGenericUDF(), Lists.newArrayList(oneColExpr, new ExprNodeConstantDesc(oneColType, 'xyz1')))
        def totalExprInvalid = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getFunctionInfo('=').getGenericUDF(), Lists.newArrayList(totalColExpr, new ExprNodeConstantDesc(totalColType, 1)))
        def oneExprAndTotalExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getFunctionInfo('and').getGenericUDF(), Lists.newArrayList(oneExpr, totalExpr))

        def result2 = []
        client.getPartitionsByExpr(hiveTable, oneExprInvalid, client.getConf(), result2)
        def result3 = []
        client.getPartitionsByExpr(hiveTable, totalExprInvalid, client.getConf(), result3)
        def result4 = []
        client.getPartitionsByExpr(hiveTable, oneExprAndTotalExpr, client.getConf(), result4)
        def result5 = []
        def oneIsNullExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
            FunctionRegistry.getFunctionInfo('isnull').getGenericUDF(), Lists.newArrayList(oneColExpr))
        try {
            client.getPartitionsByExpr(hiveTable, oneIsNullExpr, client.getConf(), result4)
        } catch (TApplicationException ignored){}
        then:
        result.size() == 10
        result1.size() == 1
        result2.size() == 0
        result3.size() == 0
        result4.size() == 1
        result5.size() == 0
        cleanup:
        def partitionNames = client.getPartitionNames(databaseName, tableName, (short) -1)
        partitionNames.each {
            client.dropPartition(databaseName, tableName, Lists.newArrayList(PartitionUtil.getPartitionKeyValues(it).values()), false)
        }
    }

    @Unroll
    def "Test: Embedded Thrift connector: get partitions for filter #filter returned #result partitions"() {
        when:
        def catalogName = 'local'
        def client = localHiveClient
        def databaseName = 'test_db5_' + catalogName
        def tableName = 'parts'
        def hiveTable = createTable(client, catalogName, databaseName, tableName)
        if (cursor == 'start') {
            def uri = isLocalEnv ? 'file:/tmp/abc' : null;
            def dto = converter.toTableDto(hiveConverter.toTableInfo(QualifiedName.ofTable(catalogName, databaseName, tableName), hiveTable.getTTable()))
            def partitionDtos = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, 'one=xyz/total=1', uri, 10)
            def partitions = partitionDtos.collect {
                new Partition(hiveTable, hiveConverter.fromPartitionInfo(converter.fromTableDto(dto), converter.fromPartitionDto(it)))
            }
            client.alterPartitions(databaseName + '.' + tableName, partitions)
        }
        then:
        try {
            client.getPartitionsByFilter(hiveTable, filter).size() == result
        } catch (Exception e) {
            result == -1
            e.message.contains('400 Bad Request')
        }
        cleanup:
        if (cursor == 'end') {
            def partitionNames = client.getPartitionNames(databaseName, tableName, (short) -1)
            partitionNames.each {
                client.dropPartition(databaseName, tableName, Lists.newArrayList(PartitionUtil.getPartitionKeyValues(it).values()), false)
            }
        }
        where:
        cursor  | filter                                 | result
        'start' | "one='xyz'"                            | 10
        ''      | 'one="xyz"'                            | 10
        ''      | "one='xyz' and one like 'xy_'"         | 10
        ''      | "(one='xyz') and one like 'xy%'"       | 10
        ''      | "one like 'xy%'"                       | 10
        ''      | "total=10"                             | 1
        ''      | "total='10'"                           | 1
        ''      | "total<1"                              | 0
        ''      | "total>1"                              | 10
        ''      | "total>=10"                            | 10
        ''      | "total<=20"                            | 10
        ''      | "total between 1 and 20"               | 10
        ''      | "total not between 1 and 20"           | 0
        ''      | 'one=xyz'                              | -1
        ''      | 'invalid=xyz'                          | -1
        'end'   | "one='xyz' and (total=11 or total=12)" | 2
    }

    @Unroll
    def "Test: Embedded Fast Thrift connector: get partitions for filter #filter returned #result partitions"() {
        when:
        def catalogName = 'localfast'
        def client = localFastHiveClient
        def databaseName = 'test_db5_' + catalogName
        def tableName = 'parts'
        def hiveTable = createTable(client, catalogName, databaseName, tableName)
        if (cursor == 'start') {
            def uri = isLocalEnv ? 'file:/tmp/abc' : null;
            def dto = converter.toTableDto(hiveConverter.toTableInfo(QualifiedName.ofTable(catalogName, databaseName, tableName), hiveTable.getTTable()))
            def partitionDtos = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, 'one=xyz/total=1', uri, 10)
            def partitions = partitionDtos.collect {
                new Partition(hiveTable, hiveConverter.fromPartitionInfo(converter.fromTableDto(dto), converter.fromPartitionDto(it)))
            }
            client.alterPartitions(databaseName + '.' + tableName, partitions)
        }
        then:
        try {
            client.getPartitionsByFilter(hiveTable, filter).size() == result
        } catch (Exception e) {
            result == -1
            e.message.contains('400 Bad Request')
        }
        cleanup:
        if (cursor == 'end') {
            def partitionNames = client.getPartitionNames(databaseName, tableName, (short) -1)
            partitionNames.each {
                client.dropPartition(databaseName, tableName, Lists.newArrayList(PartitionUtil.getPartitionKeyValues(it).values()), false)
            }
        }
        where:
        cursor  | filter                                 | result
        'start' | "one='xyz'"                            | 10
        ''      | 'one="xyz"'                            | 10
        ''      | "one='xyz' and one like 'xy_'"         | 10
        ''      | "(one='xyz') and one like 'xy%'"       | 10
        ''      | "one like 'xy%'"                       | 10
        ''      | "total=10"                             | 1
        ''      | "total='10'"                           | 1
        ''      | "total<1"                              | 0
        ''      | "total>1"                              | 10
        ''      | "total>=10"                            | 10
        ''      | "total<=20"                            | 10
        ''      | "total between 1 and 20"               | 10
        ''      | "total not between 1 and 20"           | 0
        ''      | 'one=xyz'                              | -1
        ''      | 'invalid=xyz'                          | -1
        'end'   | "one='xyz' and (total=11 or total=12)" | 2
    }

    @Unroll
    def "Test: Embedded Fast Thrift connector: getPartitionsByNames with escape values"() {
        given:
        def catalogName = 'localfast'
        def client = localFastHiveClient
        def databaseName = 'test_db5_' + catalogName
        def tableName = 'parts'
        def hiveTable = createTable(client, catalogName, databaseName, tableName)
        def uri = isLocalEnv ? 'file:/tmp/abc' : null;
        def dto = converter.toTableDto(hiveConverter.toTableInfo(QualifiedName.ofTable(catalogName, databaseName, tableName), hiveTable.getTTable()))
        def partitionDtos = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, 'one=xy^:z/total=1', uri, 10)
        def partitions = partitionDtos.collect {
            new Partition(hiveTable, hiveConverter.fromPartitionInfo(converter.fromTableDto(dto), converter.fromPartitionDto(it)))
        }
        client.alterPartitions(databaseName + '.' + tableName, partitions)
        when:
        def result = client.getPartitionsByNames(hiveTable, ['one=xy%5E%3Az/total=10'])
        then:
        result.size() == 1
        result.get(0).getValues() == ['xy^:z', '10']
        when:
        result = client.getPartitionsByNames(hiveTable, ['one=xy^:z/total=10'])
        then:
        result.size() == 0
        when:
        result = client.getPartitionsByNames(hiveTable, ['total':'10'])
        then:
        result.size() == 1
        result.get(0).getValues() == ['xy^:z', '10']
        when:
        result = client.getPartitionsByNames(hiveTable, ['one':'xy^:z'])
        then:
        result.size() == 10
        when:
        result = client.getPartitionsByNames(hiveTable, ['one':'xy%5E%3Az'])
        then:
        result.size() == 0
        cleanup:
        client.getPartitions(hiveTable).each {
            client.dropPartition(databaseName, tableName, it.getValues(), false)
        }
    }
}
