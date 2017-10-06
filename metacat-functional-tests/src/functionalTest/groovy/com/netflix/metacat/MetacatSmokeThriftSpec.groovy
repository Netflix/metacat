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
import com.netflix.metacat.common.server.converter.DozerTypeConverter
import com.netflix.metacat.common.server.converter.TypeConverterFactory
import com.netflix.metacat.common.server.partition.util.PartitionUtil
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.testdata.provider.DataDtoProvider
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.metadata.Partition
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.logging.LogManager

/**
 * Created by amajumdar on 5/12/15.
 */
class MetacatSmokeThriftSpec extends Specification {
    @Shared
    Map<String, Hive> clients = [:]
    @Shared
    ConverterUtil converter
    @Shared
    HiveConnectorInfoConverter hiveConverter

    def setupSpec() {
        Logger.getRootLogger().setLevel(Level.OFF)
        HiveConf conf = new HiveConf()
        conf.set('hive.metastore.uris', "thrift://localhost:${System.properties['metacat_hive_thrift_port']}")
        SessionState.setCurrentSessionState(new SessionState(conf))
        clients.put('remote', Hive.get(conf))
        HiveConf localConf = new HiveConf()
        localConf.set('hive.metastore.uris', "thrift://localhost:${System.properties['metacat_embedded_hive_thrift_port']}")
        SessionState.setCurrentSessionState(new SessionState(localConf))
        clients.put('local', Hive.get(localConf))
        HiveConf localFastConf = new HiveConf()
        localFastConf.set('hive.metastore.uris', "thrift://localhost:${System.properties['metacat_embedded_fast_hive_thrift_port']}")
        SessionState.setCurrentSessionState(new SessionState(localFastConf))
        clients.put('localfast', Hive.get(localFastConf))
        ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger("ROOT")).setLevel(ch.qos.logback.classic.Level.OFF)
        converter = new ConverterUtil(
            new DozerTypeConverter(
                new TypeConverterFactory(
                    new DefaultConfigImpl(
                        new MetacatProperties()
                    )
                )
            )
        )
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
        client << clients.values()
        catalogName << clients.keySet()
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
        where:
        client << clients.values()
        catalogName << clients.keySet()
    }

    @Unroll
    def "Test create table for #catalogName/#databaseName/#tableName"() {
        given:
        def databaseName = 'test_db2_' + catalogName
        def tableName = 'test_create_table'
        try {
            client.createDatabase(new Database(databaseName, 'test_db', null, null))
        } catch (Exception ignored) {
        }
        def uri = getUri(databaseName, tableName)
        def dto = DataDtoProvider.getTable(catalogName, databaseName, tableName, 'test', uri)
        def hiveTable = new Table(hiveConverter.fromTableInfo(converter.fromTableDto(dto)))
        when:
        client.createTable(hiveTable)
        then:
        def table = client.getTable(databaseName, tableName)
        assert table != null && table.getTableName() == tableName
        assert table.getSd().getLocation() == uri
        when:
        client.createTable(hiveTable)
        then:
        thrown(Exception.class)
        cleanup:
        client.dropTable(databaseName, tableName)
        where:
        client << clients.values()
        catalogName << clients.keySet()
    }

    @Unroll
    def "Test rename table for #catalogName/#databaseName/#tableName to #newTableName"() {
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
        cleanup:
        client.dropTable(databaseName, newTableName)
        where:
        client << clients.values()
        catalogName << clients.keySet()
    }

    @Unroll
    def "Test('#repeat') save partitions for #catalogName/#databaseName/#tableName with partition name starting with #partitionName"() {
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
        client << clients.values()
        catalogName << clients.keySet()
    }

    @Unroll
    def "Test: Remote Thrift connector: get partitions for filter #filter returned #result partitions"() {
        when:
        def catalogName = 'remote'
        def client = clients.get(catalogName)
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
        client.getPartitionsByFilter(hiveTable, filter).size() == (filter.contains('like') ? 0 : result)
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
    def "Test: Embedded Thrift connector: get partitions for filter #filter returned #result partitions"() {
        when:
        def catalogName = 'local'
        def client = clients.get(catalogName)
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
        def client = clients.get(catalogName)
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
}
