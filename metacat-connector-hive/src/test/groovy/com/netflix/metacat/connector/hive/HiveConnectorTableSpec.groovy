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

package com.netflix.metacat.connector.hive

import com.google.common.collect.ImmutableMap
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.*
import com.netflix.metacat.common.server.connectors.model.AuditInfo
import com.netflix.metacat.common.server.connectors.model.StorageInfo
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.connectors.model.ViewInfo
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.connector.hive.util.HiveConfigConstants
import com.netflix.metacat.testdata.provider.MetacatDataInfoProvider
import com.netflix.spectator.api.Registry
import org.apache.hadoop.hive.metastore.api.*
import org.apache.thrift.TException
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for HiveConnectorTableSpec.
 * @author zhenl
 * @since 1.0.0
 */
class HiveConnectorTableSpec extends Specification {
    @Shared
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient)
    @Shared
    HiveConnectorDatabaseService hiveConnectorDatabaseService = Mock(HiveConnectorDatabaseService)
    @Shared
    HiveConnectorTableService hiveConnectorTableService = new HiveConnectorTableService(
        "testhive",
        metacatHiveClient,
        hiveConnectorDatabaseService,
        new HiveConnectorInfoConverter(new HiveTypeConverter()),
        new ConnectorContext(
            "testHive",
            "testHive",
            "hive",
            Mock(Config),
            Mock(Registry),
            ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true")
        )
    )
    @Shared
    ConnectorRequestContext connectorRequestContext = new ConnectorRequestContext(1, null)
    @Shared
    ConnectorContext connectorContext = new ConnectorContext(
        "testHive",
        "testHive",
        "hive",
        Mock(Config),
        Mock(Registry),
        ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true")
    )

    def setupSpec() {
        metacatHiveClient.createTable(_) >> {}
        metacatHiveClient.getAllTables("test1") >> MetacatDataInfoProvider.getTables()
        metacatHiveClient.getTableByName("test1", "testtable3") >> { throw new TException() }
        metacatHiveClient.rename("test1", "testtable1", "test2", "testtable2") >> {}
        metacatHiveClient.dropTable("test1", "testtable1") >> {}
        metacatHiveClient.dropTable("test1", "testtable3") >> { throw new TException() }
        metacatHiveClient.getTableByName("test1", "testtable1") >> { getTable("testtable1") }
    }

    static getTable(String tableName) {
        Table table = new Table()
        table.tableName = tableName
        table.owner = "test"
        table.partitionKeys = new ArrayList<>()
        table.sd = new StorageDescriptor()
        table.sd.setLocation("s3://test/uri")
        table.sd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
        table.sd.setInputFormat('org.apache.hadoop.hive.ql.io.orc.OrcInputFormat')
        def serializationLib = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        def sdInfoName = 'is ignored for now'
        def sdInfoParams = ['serialization.format': '1']
        table.sd.setSerdeInfo(new SerDeInfo(sdInfoName, serializationLib, sdInfoParams));
        table.sd.cols = new ArrayList<>()
        table.sd.cols.add(new FieldSchema("coldate", "date", ""))
        table.parameters = ['tp_k1': 'tp_v1']
        return table
    }

    static getPartitionTable(String tableName) {
        Table table = new Table()
        table.dbName = "test1"
        table.tableName = tableName
        table.owner = "test"
        table.partitionKeys = new ArrayList<>()
        table.sd = new StorageDescriptor()
        table.sd.setLocation("s3://test/uri")
        table.sd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
        table.sd.setInputFormat('org.apache.hadoop.hive.ql.io.orc.OrcInputFormat')
        def serializationLib = 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
        def sdInfoName = 'is ignored for now'
        def sdInfoParams = ['serialization.format': '1']
        table.sd.setSerdeInfo(new SerDeInfo(sdInfoName, serializationLib, sdInfoParams));
        table.sd.cols = new ArrayList<>()
        FieldSchema hour = new FieldSchema("hour", "string", "")
        FieldSchema date = new FieldSchema("dateint", "string", "")
        table.sd.cols.add(new FieldSchema("coldstring", "string", ""))
        table.addToPartitionKeys(date)
        table.addToPartitionKeys(hour)
        return table
    }

    def "Test for create table"() {
        when:
        hiveConnectorTableService.create(connectorRequestContext,
            TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testingtable"))
                .serde(StorageInfo.builder().serializationLib('org.apache.hadoop.hive.ql.io.orc.OrcSerde').outputFormat('org.apache.hadoop.hive.ql.io.orc.OrcInputFormat').build()).build())
        then:
        noExceptionThrown()
    }

    def "Test for create virtual view"() {
        when:
        hiveConnectorTableService.create(connectorRequestContext,
            TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testingview"))
                .view(ViewInfo.builder().viewOriginalText("test sql").build())
                .serde(StorageInfo.builder().serializationLib('org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')
                .outputFormat('org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat')
                .build()).build())
        then:
        noExceptionThrown()
    }

    def "Test for create virtual view with missing viewText"() {
        def client = Mock(MetacatHiveClient)
        def hiveConnectorTableService = new HiveConnectorTableService("testhive", client, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)

        when:
        hiveConnectorTableService.create(connectorRequestContext,
            TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testingview"))
                .view(ViewInfo.builder().viewOriginalText("").build())
                .serde(StorageInfo.builder().serializationLib('org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe')
                .outputFormat('org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat')
                .build()).build())
        then:
        1 * client.createTable(_) >> { throw new MetaException("testing message") }
        thrown InvalidMetaException
    }

    @Unroll
    def "Test for create table throw exception"() {
        def client = Mock(MetacatHiveClient)
        def hiveConnectorTableService = new HiveConnectorTableService("testhive", client, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)

        when:
        hiveConnectorTableService.create(connectorRequestContext,
            TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testingtable"))
                .serde(StorageInfo.builder().serializationLib('org.apache.hadoop.hive.ql.io.orc.OrcSerde').outputFormat('org.apache.hadoop.hive.ql.io.orc.OrcInputFormat').build()).build())
        then:
        1 * client.createTable(_) >> { throw exception }
        thrown result

        where:
        exception                                                         | result
        new TException()                                                  | ConnectorException
        new org.apache.hadoop.hive.metastore.api.AlreadyExistsException() | TableAlreadyExistsException
        new MetaException("testing ")                                     | InvalidMetaException
        new InvalidObjectException("not a valid object name")             | InvalidMetaException
        new InvalidObjectException("test1")                               | DatabaseNotFoundException
    }

    @Unroll
    def "Test for update table"() {
        given:
        hiveConnectorTableService.updateTable(connectorRequestContext, table, tableInfo)
        expect:
        table.getParameters().get("EXTERNAL") == "TRUE"
        table.getParameters() != null
        table.getSd() != null
        where:
        table                           | tableInfo
        getPartitionTable("testtable")  | TableInfo.builder()
            .name(QualifiedName.ofTable("testhive", "test1", "testtable1"))
            .serde(StorageInfo.builder().owner("test").uri("s3://test/uri").build())
            .metadata(['tp_k1': 'tp_v1'])
            .auditInfo(AuditInfo.builder().build())
            .build()
        getPartitionTable("testtable2") | TableInfo.builder()
            .name(QualifiedName.ofTable("testhive", "test1", "testtable2"))
            .serde(StorageInfo.builder().owner("test").uri("s3://test/uri").build())
            .auditInfo(AuditInfo.builder().build())
            .build()
        getPartitionTable("testtable3") | TableInfo.builder()
            .name(QualifiedName.ofTable("testhive", "test1", "testtable3"))
            .build()
    }

    def "Test for get table"() {
        when:
        def name = QualifiedName.ofTable("testhive", "test1", "testtable1")
        def tableInfo = hiveConnectorTableService.get(connectorRequestContext, name)
        def expected = MetacatDataInfoProvider.getTableInfo("testtable1")

        then:
        tableInfo == expected
    }

    @Unroll
    def "Test for get table with exceptions"() {
        def client = Mock(MetacatHiveClient)
        def hiveConnectorTableService = new HiveConnectorTableService("testhive", client, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)

        when:
        hiveConnectorTableService.get(connectorRequestContext, QualifiedName.ofTable("testhive", "test1", "testtable3"))
        then:
        1 * client.getTableByName(_, _) >> { throw exception }
        thrown result

        where:
        exception                   | result
        new TException()            | ConnectorException
        new MetaException()         | InvalidMetaException
        new NoSuchObjectException() | TableNotFoundException
    }

    def "Test for delete table"() {
        when:
        def name = QualifiedName.ofTable("testhive", "test1", "testtable1")
        hiveConnectorTableService.delete(connectorRequestContext, name)
        then:
        noExceptionThrown()
    }

    @Unroll
    def "Test for delete table throw exceptions"() {
        def client = Mock(MetacatHiveClient)
        def hiveConnectorTableService = new HiveConnectorTableService("testhive", client, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)

        when:
        def name = QualifiedName.ofTable("testhive", "test1", "testtable3")
        hiveConnectorTableService.delete(connectorRequestContext, name)
        then:
        1 * client.dropTable(_, _) >> { throw exception }
        thrown result

        where:
        exception                   | result
        new TException()            | ConnectorException
        new MetaException()         | InvalidMetaException
        new NoSuchObjectException() | TableNotFoundException
    }

    @Unroll
    def "Test for listNames tables"() {
        when:
        def tables = hiveConnectorTableService.listNames(connectorRequestContext, QualifiedName.ofDatabase("testhive", "test1"), null, order, null)
        then:
        tables == result
        where:
        order                          | result
        new Sort(null, SortOrder.ASC)  | MetacatDataInfoProvider.getAllTableNames()
        new Sort(null, SortOrder.DESC) | MetacatDataInfoProvider.getAllTableNames().reverse()
    }

    @Unroll
    def "Test for listNames tables exceptions"() {
        def client = Mock(MetacatHiveClient)
        def hiveConnectorTableService = new HiveConnectorTableService("testhive", client, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)

        when:
        hiveConnectorTableService.listNames(connectorRequestContext, QualifiedName.ofDatabase("testhive", "test1"), null, null, null)

        then:
        1 * client.getAllTables(_) >> { throw exception }
        thrown result

        where:
        exception                   | result
        new TException()            | ConnectorException
        new MetaException()         | InvalidMetaException
        new NoSuchObjectException() | DatabaseNotFoundException
    }

    @Unroll
    def "Test for exist table"() {
        def client = Mock(MetacatHiveClient)
        def hiveConnectorTableService = new HiveConnectorTableService("testhive", client, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)
        when:
        def result = hiveConnectorTableService.exists(connectorRequestContext, QualifiedName.ofTable("testhive", "testdb", "testtable1"))
        then:
        1 * client.getTableByName("testdb", "testtable1") >> resulttble
        result == ret
        where:
        resulttble  | ret
        new Table() | true
        null        | false
    }

    def "Test for exist table NoSuchObjectException"() {
        def client = Mock(MetacatHiveClient)
        def hiveConnectorTableService = new HiveConnectorTableService("testhive", client, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)
        when:
        def result = hiveConnectorTableService.exists(connectorRequestContext, QualifiedName.ofTable("testhive", "testdb", "testtable1"))
        then:
        1 * client.getTableByName("testdb", "testtable1") >> { throw new NoSuchObjectException() }
        !result
    }

    def "Test for exist table TException"() {
        def client = Mock(MetacatHiveClient)
        def hiveConnectorTableService = new HiveConnectorTableService("testhive", client, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)
        when:
        hiveConnectorTableService.exists(connectorRequestContext, QualifiedName.ofTable("testhive", "testdb", "testtable1"))
        then:
        client.getTableByName("testdb", "testtable1") >> { throw new TException() }
        thrown ConnectorException
    }

    @Unroll
    def "Test for listNames tables with page"() {
        given:
        def tbls = hiveConnectorTableService.listNames(
            connectorRequestContext, QualifiedName.ofDatabase("testhive", "test1"), prefix, null, pageable)

        expect:
        tbls == result

        where:
        prefix                                             | pageable           | result
        null                                               | new Pageable(2, 1) | [QualifiedName.ofTable("testhive", "test1", "testtable2"), QualifiedName.ofTable("testhive", "test1", "devtable2")]
        QualifiedName.ofDatabase("testhive", "test1")      | new Pageable(2, 1) | [QualifiedName.ofTable("testhive", "test1", "testtable2"), QualifiedName.ofTable("testhive", "test1", "devtable2")]
        QualifiedName.ofTable("testhive", "test1", "test") | new Pageable(2, 1) | [QualifiedName.ofTable("testhive", "test1", "testtable2")]
        QualifiedName.ofTable("testhive", "test1", "test") | new Pageable(1, 0) | [QualifiedName.ofTable("testhive", "test1", "testtable1")]
        QualifiedName.ofTable("testhive", "test1", "test") | new Pageable(0, 0) | []
        QualifiedName.ofTable("testhive", "test3", "test") | new Pageable(2, 1) | []
    }

    def "Test for rename tables"() {
        when:
        hiveConnectorTableService.rename(
            connectorRequestContext, QualifiedName.ofTable("testhive", "test1", "testtable1"),
            QualifiedName.ofTable("testhive", "test1", "testtable2"))
        then:
        noExceptionThrown()
    }

    @Unroll
    def "Test for rename tables exceptions"() {
        def client = Mock(MetacatHiveClient)
        def hiveConnectorTableService = new HiveConnectorTableService("testhive", client, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)

        when:
        hiveConnectorTableService.rename(
            connectorRequestContext, QualifiedName.ofTable("testhive", "test1", "testtable1"),
            QualifiedName.ofTable("testhive", "test1", "testtable2"))
        then:
        1 * client.getTableByName(_,_) >> new Table()
        1 * client.rename(_, _, _, _) >> { throw exception }
        thrown result

        where:
        exception                   | result
        new TException()            | ConnectorException
        new MetaException()         | InvalidMetaException
        new NoSuchObjectException() | TableNotFoundException
    }
}
