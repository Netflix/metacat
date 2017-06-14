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
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.model.*
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import com.netflix.metacat.common.server.connectors.exception.PartitionAlreadyExistsException
import com.netflix.metacat.common.server.connectors.exception.PartitionNotFoundException
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.InvalidObjectException
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.Partition
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.thrift.TException
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for HiveConnectorPartitionSpec.
 * @author zhenl
 * @since 1.0.0
 */
class HiveConnectorPartitionSpec extends Specification{
    @Shared
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient);
    @Shared
    HiveConnectorPartitionService hiveConnectorPartitionService = new HiveConnectorPartitionService("testhive", metacatHiveClient, new HiveConnectorInfoConverter(new HiveTypeConverter()) )
    @Shared
    ConnectorRequestContext connectorContext = new ConnectorRequestContext(1, null);
    @Shared
    HiveConnectorInfoConverter hiveConnectorInfoConverter = new HiveConnectorInfoConverter( new HiveTypeConverter())

    def setupSpec() {
        metacatHiveClient.getTableByName("test1", "testtable1") >> { HiveConnectorTableSpec.getPartitionTable("testtable1")}
        metacatHiveClient.getTableByName("test1", "testtable2") >> { HiveConnectorTableSpec.getPartitionTable("testtable2")}
        metacatHiveClient.getPartitions("test1", "testtable2", null) >> { getPartition("test1", "testtable2")}
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
    def "Test for get partitions exceptions" (){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorPartitionService = new HiveConnectorPartitionService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )
        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest();
        partitionListRequest.partitionNames = [
                "dateint=20170101/hour=1"
        ]
        hiveConnectorPartitionService.getPartitions( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)

        then:
        1 * client.getPartitions(_,_,_) >> { throw exception}
        thrown result

        where:
        exception                    | result
        new TException()             |ConnectorException
        new NoSuchObjectException()  |TableNotFoundException
        new MetaException()          |InvalidMetaException
        new InvalidObjectException() |InvalidMetaException
    }

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

    def "Test for getNameOfPartitions" (){
        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest();
        def partitionNames = hiveConnectorPartitionService.getPartitionKeys( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)
        def expected = [ "dateint=20170101/hour=1", "dateint=20170101/hour=2","dateint=20170101/hour=3" ]
        then:
        partitionNames == expected
    }

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
    def "Test for getNameOfPartitions exceptions" (){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorPartitionService = new HiveConnectorPartitionService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )

        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest()
        partitionListRequest.partitionNames = [
                "dateint=20170101/hour=1", "dateint=20170101/hour=2"
        ]
        hiveConnectorPartitionService.getPartitionKeys( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)
        then:
        1 * client.getTableByName(_,_) >> { throw exception}
        thrown result

        where:
        exception                    | result
        new TException()             |ConnectorException
        new NoSuchObjectException()  |TableNotFoundException
        new MetaException()          |InvalidMetaException
        new InvalidObjectException() |InvalidMetaException
    }

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

    def "Test for getPartitionKeys" (){
        when:
        PartitionListRequest partitionListRequest = new PartitionListRequest();
        partitionListRequest.partitionNames = [
            "dateint=20170101/hour=1", "dateint=20170101/hour=2"
        ]
        partitionListRequest.sort = new Sort(null, SortOrder.DESC)
        def partitionKeys = hiveConnectorPartitionService.getPartitionKeys( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"), partitionListRequest)
        def expected = [ "dateint=20170101/hour=2", "dateint=20170101/hour=1" ]
        then:
        partitionKeys == expected
    }

    def "Test for getPartitionCount" (){
        when:
        def count = hiveConnectorPartitionService.getPartitionCount( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"))
        then:
        count == 3
    }

    @Unroll
    def "Test for getPartitionCount exceptions" (){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorPartitionService = new HiveConnectorPartitionService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )

        when:
        hiveConnectorPartitionService.getPartitionCount( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"))
        then:
        1 * client.getPartitionCount(_,_) >> { throw exception}
        thrown result

        where:
        exception                    | result
        new TException()             |ConnectorException
        new NoSuchObjectException()  |TableNotFoundException
        new MetaException()          |InvalidMetaException
        new InvalidObjectException() |InvalidMetaException
    }

    def "Test for savePartition" (){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorPartitionService = new HiveConnectorPartitionService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )
        when:
        def saverequest = new PartitionsSaveRequest()
        saverequest.checkIfExists = true
        saverequest.alterIfExists = true
        saverequest.partitions = [ PartitionInfo.builder().name(QualifiedName.ofPartition("testhive", "test1", "testtable2", "dateint=20170101/hour=3")).build() ]
        def saveresponse = hiveConnectorPartitionService.savePartitions( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"),saverequest)

        then:
        1 * client.getTableByName("test1", "testtable2") >> { HiveConnectorTableSpec.getPartitionTable("testtable2") }
        0 * client.alterPartitions("test1","testtable2",_)
        1 * client.addDropPartitions(_,_,_,_)
        noExceptionThrown()
    }

    @Unroll
    def "Test for savePartition exceptions" (){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorPartitionService = new HiveConnectorPartitionService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )

        when:
        def saverequest = new PartitionsSaveRequest()
        saverequest.partitions = [ PartitionInfo.builder().name(QualifiedName.ofPartition("testhive", "test1", "testtable2", "dateint=20170101/hour=3")).build() ]
        def saveresponse = hiveConnectorPartitionService.savePartitions( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"),saverequest)
        then:
        1 * client.getTableByName("test1", "testtable2") >> { throw exception}
        thrown result

        where:
        exception                                                     | result
        new TException()                                              |ConnectorException
        new NoSuchObjectException("Database")                         |TableNotFoundException
        new NoSuchObjectException("Partition doesn't exist")          |PartitionNotFoundException
        new MetaException()          |InvalidMetaException
        new InvalidObjectException() |InvalidMetaException
    }

    def "Test for savePartition partitionalreadyexist exception" (){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorPartitionService = new HiveConnectorPartitionService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )
        when:
        def saverequest = new PartitionsSaveRequest()
        saverequest.checkIfExists = true
        saverequest.alterIfExists = true
        saverequest.partitions = [ PartitionInfo.builder().name(QualifiedName.ofPartition("testhive", "test1", "testtable2", "dateint=20170101/hour=3")).build() ]
        def saveresponse = hiveConnectorPartitionService.savePartitions( connectorContext, QualifiedName.ofTable("testhive", "test1", "testtable2"),saverequest)

        then:
        1 * client.getTableByName("test1", "testtable2") >> { HiveConnectorTableSpec.getPartitionTable("testtable2") }
        0 * client.alterPartitions("test1","testtable2",_)
        1 * client.addDropPartitions(_,_,_,_) >> { throw new AlreadyExistsException()}
        final PartitionAlreadyExistsException exception = thrown()
        exception.name == QualifiedName.ofTable("testhive", "test1", "testtable2")
    }
}
