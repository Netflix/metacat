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

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.model.AuditInfo
import com.netflix.metacat.common.server.connectors.model.StorageInfo
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.exception.InvalidMetaException
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.common.server.exception.ConnectorException
import com.netflix.metacat.connector.hive.thrift.MetacatHiveClient
import com.netflix.metacat.testdata.provider.MetacatDataInfoProvider
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.thrift.TException
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for HiveConnectorTableSpec.
 * @author zhenl
 */
class HiveConnectorTableSpec extends Specification {
    @Shared
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient);
    @Shared
    HiveConnectorDatabaseService hiveConnectorDatabaseService = Mock(HiveConnectorDatabaseService);
    @Shared
    HiveConnectorTableService hiveConnectorTableService = new HiveConnectorTableService("testhive", metacatHiveClient, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()) )
    @Shared
    ConnectorContext connectorContext = new ConnectorContext(1, null);
    @Shared
    HiveConnectorInfoConverter hiveConnectorInfoConverter = new HiveConnectorInfoConverter(new HiveTypeConverter())

    def setupSpec() {
        metacatHiveClient.createTable(_) >> {}
        metacatHiveClient.getAllTables("test1") >> MetacatDataInfoProvider.getTables()
        metacatHiveClient.getTableByName("test1", "testtable3") >> { throw new TException()}
        metacatHiveClient.rename("test1", "testtable1", "test2", "testtable2") >> {}
        metacatHiveClient.dropTable("test1", "testtable1") >> {}
        metacatHiveClient.dropTable("test1", "testtable3") >> {throw new TException()}
        metacatHiveClient.getTableByName("test1", "testtable1") >> { getTable("testtable1")}
    }

    static getTable(String tableName){
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

    static getPartitionTable(String tableName){
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
        FieldSchema hour = new FieldSchema("hour", "string", "")
        FieldSchema date = new FieldSchema("dateint", "string", "")
        table.sd.cols.add(new FieldSchema("coldstring", "string", ""))
        table.addToPartitionKeys(date)
        table.addToPartitionKeys(hour)
        return table
    }

    def "Test for create table" (){
        when:
        hiveConnectorTableService.create( connectorContext,
                TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testingtable"))
                        .serde(StorageInfo.builder().serializationLib('org.apache.hadoop.hive.ql.io.orc.OrcSerde').outputFormat('org.apache.hadoop.hive.ql.io.orc.OrcInputFormat').build()).build())
        then:
        noExceptionThrown()
    }

    def "Test for create table throw exception" (){
        when:
        hiveConnectorTableService.create( connectorContext,
                TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testingtable")).build())
        then:
        thrown InvalidMetaException
    }

    @Unroll
    def "Test for update table" (){
        given:
            hiveConnectorTableService.updateTable( connectorContext, table, tableInfo)
        expect:
            table.getParameters().get("EXTERNAL") == "TRUE"
            table.getParameters() != null
            table.getSd() != null
        where:
        table                           | tableInfo
        getPartitionTable("testtable")  | TableInfo.builder()
                                          .name(QualifiedName.ofTable("testhive", "test1", "testtable1"))
                                          .serde( StorageInfo.builder().owner("test").uri("s3://test/uri").build())
                                          .metadata ( ['tp_k1': 'tp_v1'])
                                          .auditInfo( AuditInfo.builder().build())
                                          .build()
        getPartitionTable("testtable2")  | TableInfo.builder()
                .name(QualifiedName.ofTable("testhive", "test1", "testtable2"))
                .serde( StorageInfo.builder().owner("test").uri("s3://test/uri").build())
                .auditInfo( AuditInfo.builder().build())
                .build()
        getPartitionTable("testtable3")  | TableInfo.builder()
                .name(QualifiedName.ofTable("testhive", "test1", "testtable3"))
                .build()
    }

    def "Test for get table" (){
        when:
        def name = QualifiedName.ofTable("testhive", "test1", "testtable1")
        def tableInfo = hiveConnectorTableService.get( connectorContext, name)
        def expected = MetacatDataInfoProvider.getTableInfo("testtable1")

        then:
            tableInfo == expected
    }

    def "Test for get table with exception" (){
        when:
        hiveConnectorTableService.get( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable3"))
        then:
        thrown ConnectorException
    }

    def "Test for delete table" (){
        when:
        def name = QualifiedName.ofTable("testhive", "test1", "testtable1")
        hiveConnectorTableService.delete( connectorContext, name)
        then:
        noExceptionThrown()
    }

    def "Test for delete table throw exception" (){
        when:
        def name = QualifiedName.ofTable("testhive", "test1", "testtable3")
        hiveConnectorTableService.delete( connectorContext, name)
        then:
        thrown ConnectorException
    }

    def "Test for listNames tables"(){
        when:
        def tables = hiveConnectorTableService.listNames(connectorContext, QualifiedName.ofDatabase("testhive","test1"), null, null, null )
        then:
        tables == MetacatDataInfoProvider.getAllTableNames()
    }

    @Unroll
    def "Test for listNames tables with page"(){
        given:
        def tbls = hiveConnectorTableService.listNames(
            connectorContext, QualifiedName.ofDatabase("testhive", "test1"), prefix, null, pageable)

        expect:
        tbls == result

        where:
        prefix                                             | pageable          | result
        null | new Pageable(2,1) |[QualifiedName.ofTable("testhive", "test1", "testtable2"),QualifiedName.ofTable("testhive", "test1", "devtable2")]
        QualifiedName.ofDatabase("testhive", "test1") | new Pageable(2,1) |[QualifiedName.ofTable("testhive", "test1", "testtable2"),QualifiedName.ofTable("testhive", "test1", "devtable2")]
        QualifiedName.ofTable("testhive", "test1", "test") | new Pageable(2,1) |[QualifiedName.ofTable("testhive", "test1", "testtable2")]
        QualifiedName.ofTable("testhive", "test1", "test") | new Pageable(1,0) |[QualifiedName.ofTable("testhive", "test1", "testtable1")]
        QualifiedName.ofTable("testhive", "test1", "test") | new Pageable(0,0) |[]
        QualifiedName.ofTable("testhive", "test3", "test") | new Pageable(2,1) |[]
    }

    def "Test for rename tables"() {
        when:
        hiveConnectorTableService.rename(
            connectorContext, QualifiedName.ofTable("testhive", "test1","testtable1"),
            QualifiedName.ofTable("testhive", "test1", "testtable2"))
        then:
        noExceptionThrown()
    }

}
