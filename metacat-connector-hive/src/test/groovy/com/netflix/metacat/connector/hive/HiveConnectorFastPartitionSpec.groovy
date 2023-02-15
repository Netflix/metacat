/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.connector.hive


import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import com.netflix.metacat.testdata.provider.DataDtoProvider
import org.apache.iceberg.ScanSummary
import org.apache.iceberg.Table
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler
import com.netflix.metacat.connector.hive.iceberg.IcebergTableWrapper
import com.netflix.metacat.connector.hive.sql.DirectSqlGetPartition
import com.netflix.metacat.connector.hive.sql.DirectSqlSavePartition
import com.netflix.metacat.connector.hive.sql.HiveConnectorFastPartitionService
import com.netflix.metacat.testdata.provider.MetacatDataInfoProvider
import org.apache.hadoop.hive.metastore.Warehouse
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class HiveConnectorFastPartitionSpec extends Specification {
    static def p1 = new PartitionInfo(name: QualifiedName.fromString("c/d/t/dateint=20170101/hour=1"))
    static def p2 = new PartitionInfo(name: QualifiedName.fromString("c/d/t/dateint=20170101/hour=2"))

    @Shared
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient);
    @Shared
    DirectSqlGetPartition directSqlGetPartition = Mock(DirectSqlGetPartition)
    @Shared
    DirectSqlSavePartition directSqlSavePartition = Mock(DirectSqlSavePartition)
    @Shared
    ConnectorRequestContext connectorContext = new ConnectorRequestContext(timestamp:1)
    @Shared
    IcebergTableHandler icebergTableHandler = Mock(IcebergTableHandler)
    @Shared
    conf = Mock(Config)

    @Shared
    context = DataDtoProvider.newContext(conf, null)
    @Shared
    HiveConnectorFastPartitionService hiveConnectorFastPartitionService =
        new HiveConnectorFastPartitionService(context,
            metacatHiveClient,
            Mock(Warehouse),
            new HiveConnectorInfoConverter(new HiveTypeConverter()),
            directSqlGetPartition,
            directSqlSavePartition, icebergTableHandler)

    @Shared
    metric1 = Mock(ScanSummary.PartitionMetrics)

    @Shared
    metric2 = Mock(ScanSummary.PartitionMetrics)

    @Shared
    metric3 = Mock(ScanSummary.PartitionMetrics)

    def setupSpec() {
        conf.icebergEnabled >> true
        metric1.fileCount() >> 1
        metric1.dataTimestampMillis() >> 1234500000
        metric1.recordCount() >> 1
        metric2.fileCount() >> 1
        metric2.dataTimestampMillis() >> 2734500000
        metric2.recordCount() >> 1
        metric3.fileCount() >> 1
        metric3.dataTimestampMillis() >> null
        metric3.recordCount() >> 1

        icebergTableHandler.getPartitions(_,_,_,_,_) >> [p1, p2]
        icebergTableHandler.getIcebergTable(_,_,_) >> new IcebergTableWrapper(Mock(Table), [:])
        icebergTableHandler.getIcebergTablePartitionMap(_,_,_) >> ["dateint=20170101/hour=1": metric1,
                                                                   "dateint=20170102/hour=1": metric1,
                                                                   "dateint=20170103/hour=1": metric1,
                                                                   "dateint=20190102/hour=1": metric2,
                                                                   "dateint=20190103/hour=1": metric3]
    }

    @Unroll
    def "Test for get iceberg table partitionMaps" () {
        given:
        def tableInfo = MetacatDataInfoProvider.getIcebergTableInfo("icebergtable")
        def tableName = QualifiedName.ofTable("testhive", "test1", "icebergtable")

        when:
        def partionInfos = hiveConnectorFastPartitionService.getPartitions(
            connectorContext,
            tableName,
            new PartitionListRequest(
                pageable: new Pageable(
                    limit: limit,
                    offset: offset
                )
            ),
            tableInfo)

        then:
        partionInfos == expectedPartitions

        where:
        partitions | limit | offset || expectedPartitions
        [p1, p2]   | 1     | 0      || [p1]
        [p1, p2]   | 2     | 0      || [p1, p2]
        [p1, p2]   | 1     | 1      || [p2]
    }

    def "Test for get iceberg table partitions" (){
        given:
        def tableName = QualifiedName.ofTable("testhive", "test1", "icebergtable")
        def tableInfo = MetacatDataInfoProvider.getIcebergTableInfo("icebergtable")
        def partitionListRequest = new PartitionListRequest();
        when:
        partitionListRequest.partitionNames = [
            "dateint=20170101/hour=1",
            "dateint=20170101/hour=2"
        ]
        def partKeys = hiveConnectorFastPartitionService.getPartitionKeys(connectorContext, tableName, partitionListRequest, tableInfo)
        then:
        partKeys== [
            "dateint=20170101/hour=1",
            "dateint=20170101/hour=2"
        ]

        when:
        hiveConnectorFastPartitionService.getPartitionCount(connectorContext, tableName, tableInfo)
        then:
        thrown(UnsupportedOperationException)

        when:
        hiveConnectorFastPartitionService.getPartitionUris(connectorContext, tableName, partitionListRequest, tableInfo)
        then:
        thrown(UnsupportedOperationException)

        when:
        hiveConnectorFastPartitionService.deletePartitions(connectorContext, tableName, ['field1=true'], tableInfo)
        then:
        thrown(UnsupportedOperationException)
    }


}
