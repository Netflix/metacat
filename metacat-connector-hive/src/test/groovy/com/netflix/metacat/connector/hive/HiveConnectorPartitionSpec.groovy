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
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.model.AuditInfo
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest
import com.netflix.metacat.common.server.connectors.model.StorageInfo
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.Partition
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for HiveConnectorPartitionSpec.
 * @author zhenl
 */
class HiveConnectorPartitionSpec extends Specification{
    @Shared
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient);
    @Shared
    HiveConnectorPartitionService hiveConnectorPartitionService = new HiveConnectorPartitionService(metacatHiveClient, new HiveConnectorInfoConverter() )
    @Shared
    ConnectorContext connectorContext = new ConnectorContext(1, null);
    @Shared
    HiveConnectorInfoConverter hiveConnectorInfoConverter = new HiveConnectorInfoConverter()

    def setupSpec() {
       // metacatHiveClient.getPartition("testdb", "test1", "dateint=20170101/hour=1") >> { getPartition("test", "testtable1", "dateint=20170101/hour=1")}
        metacatHiveClient.getTableByName("test1", "testtable1") >> { HiveConnectorTableSpec.getPartitionTable("testtable1")}
        metacatHiveClient.getTableByName("test1", "testtable2") >> { HiveConnectorTableSpec.getPartitionTable("testtable2")}
        metacatHiveClient.listAllPartitions("test1", "testtable2") >> { getPartition("test1", "testtable2")}
        metacatHiveClient.getPartitionNames("test1", "testtable2") >> {return [ "dateint=20170101/hour=1", "dateint=20170101/hour=2","dateint=20170101/hour=3"] }
        metacatHiveClient.getPartitionCount("test1", "testtable2") >> {return 3}
        metacatHiveClient.savePartitions(_) >> {}
    }

    static getPartition(String databaseName, String tableName){
        List<FieldSchema> fieldSchemas = new ArrayList<>()
        fieldSchemas.add( new FieldSchema("string", "string", ""))
        String uri = "s3://test/location"
        StorageDescriptor sd = new StorageDescriptor()
        sd.setCols(fieldSchemas)
        sd.setLocation(uri)

        return [ new Partition(["20170101","1"], databaseName, tableName, 12345, 123456, sd, Collections.emptyMap()),
                 new Partition(["20170101","2"], databaseName, tableName, 12345, 123456, sd, Collections.emptyMap()),
                 new Partition(["20170101","3"], databaseName, tableName, 12345, 123456, sd, Collections.emptyMap())
            ]
    }

    static getPartitionInfo(String databaseName, String tableName, String partitionName){
        AuditInfo auditInfo = AuditInfo.builder()
            .createdDate(HiveConnectorInfoConverter.epochSecondsToDate(12345))
            .lastModifiedDate(HiveConnectorInfoConverter.epochSecondsToDate(123456)).build()

        return PartitionInfo.builder()
            .name(QualifiedName.ofPartition("testhive", databaseName, tableName, partitionName))
            .metadata(Collections.emptyMap())
            .auditInfo(auditInfo)
            .serde(StorageInfo.builder().owner("test").uri("s3://test/location").build())
            .build()
        }

    @Unroll
    def "Test for get partitions with prefix" (){
        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest();
        partitionListRequest.partitionNames = [
            "dateint=20170101/hour=1"
        ]
        def partionInfos = hiveConnectorPartitionService.getPartitions( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)
        def expected = [ getPartitionInfo("test1", "testtable2", "dateint=20170101/hour=1")]
        then:
        partionInfos == expected
    }

    @Unroll
    def "Test for get partitions" (){
        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest();
        def partionInfos = hiveConnectorPartitionService.getPartitions( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)
        def expected = [ getPartitionInfo("test1", "testtable2", "dateint=20170101/hour=1"),
                         getPartitionInfo("test1", "testtable2", "dateint=20170101/hour=2"),
                         getPartitionInfo("test1", "testtable2", "dateint=20170101/hour=3") ]
        then:
        partionInfos == expected
    }

    @Unroll
    def "Test for getNameOfPartitions" (){
        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest();
        def partitionNames = hiveConnectorPartitionService.getPartitionKeys( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)
        def expected = [ "dateint=20170101/hour=1", "dateint=20170101/hour=2","dateint=20170101/hour=3" ]
        then:
        partitionNames == expected
    }

    @Unroll
    def "Test for getNameOfPartitions with partitionIds" (){
        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest();
        partitionListRequest.partitionNames = [
            "dateint=20170101/hour=1", "dateint=20170101/hour=2"
        ]
        def partitionNames = hiveConnectorPartitionService.getPartitionKeys( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)
        def expected = [ "dateint=20170101/hour=1", "dateint=20170101/hour=2" ]
        then:
        partitionNames == expected
    }

    @Unroll
    def "Test for getPartitionUris" (){
        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest();
        partitionListRequest.partitionNames = [
            "dateint=20170101/hour=1", "dateint=20170101/hour=2"
        ]
        def partitionUris = hiveConnectorPartitionService.getPartitionUris( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)
        def expected = [ "s3://test/location", "s3://test/location" ]
        then:
        partitionUris == expected
    }

    @Unroll
    def "Test for getPartitionKeys" (){
        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest();
        partitionListRequest.partitionNames = [
            "dateint=20170101/hour=1", "dateint=20170101/hour=2"
        ]
        def partitionKeys = hiveConnectorPartitionService.getPartitionKeys( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)
        def expected = [ "dateint=20170101/hour=1", "dateint=20170101/hour=2" ]
        then:
        partitionKeys == expected
    }

    @Unroll
    def "Test for getPartitionCount" (){
        when:
        def count = hiveConnectorPartitionService.getPartitionCount( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"))
        then:
        count == 3
    }

    @Unroll
    def "Test for savePartition" (){
        when:
        def saverequest = new PartitionsSaveRequest()
        saverequest.partitions = [ PartitionInfo.builder().name(QualifiedName.ofPartition("testhive", "test1", "testtable2", "dateint=20170101/hour=3")).build() ]
        def saveresponse = hiveConnectorPartitionService.savePartitions( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"),saverequest)
        then:
        noExceptionThrown()

    }
}
