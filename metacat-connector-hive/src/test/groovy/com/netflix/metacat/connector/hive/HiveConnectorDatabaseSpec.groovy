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
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.testdata.provider.MetacatDataInfoProvider
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.InvalidObjectException
import org.apache.hadoop.hive.metastore.api.MetaException
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.thrift.TException
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for HiveConnectorDatabaseSpec.
 * @author zhenl
 * @since 1.0.0
 */
class HiveConnectorDatabaseSpec extends Specification{
    @Shared
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient);
    @Shared
    HiveConnectorDatabaseService hiveConnectorDatabaseService = new HiveConnectorDatabaseService("testhive", metacatHiveClient, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )
    @Shared
    ConnectorContext connectorContext = new ConnectorContext(1, null);

    def setupSpec() {
        metacatHiveClient.getAllDatabases() >> ["test1", "test2", "dev1", "dev2"]
        metacatHiveClient.getDatabase("testdb2") >> { throw new TException() }
        metacatHiveClient.createDatabase(new Database("testdb3","testdb3","file://temp/",Collections.EMPTY_MAP)) >> {}
    }

    def "Test for create database" (){
        when:
            hiveConnectorDatabaseService.create( connectorContext, DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "testdb3")).build())
        then:
        noExceptionThrown()
    }

    @Unroll
    def "Test for create database with Exceptions" (){
        def client = Mock(MetacatHiveClient);
        def hiveConnectorDatabaseService = new HiveConnectorDatabaseService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )
        when:
        hiveConnectorDatabaseService.create( connectorContext, DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "testdb2")).uri("file://temp/").build())
        then:
        1 * client.createDatabase(new Database("testdb2","testdb2","file://temp/",Collections.EMPTY_MAP)) >> { throw exception}
        thrown result

        where:
        exception                    | result
        new TException()             |ConnectorException
        new AlreadyExistsException() |DatabaseAlreadyExistsException
        new MetaException()          |InvalidMetaException
        new InvalidObjectException() |InvalidMetaException
    }

    def "Test for listNames database"(){
        when:
        def dbs = hiveConnectorDatabaseService.listNames(connectorContext, QualifiedName.ofDatabase("testhive","testdb"), null, order, null )
        then:
        dbs == result
        where:
        order | result
        new Sort(null, SortOrder.ASC) | MetacatDataInfoProvider.getAllDatabaseNames()
        new Sort(null, SortOrder.DESC)| MetacatDataInfoProvider.getAllDatabaseNames().reverse()
    }

    @Unroll
    def "Test for exist database"(){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorDatabaseService = new HiveConnectorDatabaseService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )
        when:
        def result = hiveConnectorDatabaseService.exists(connectorContext, QualifiedName.ofDatabase("testhive","testdb"))
        then:
        client.getDatabase(_) >> resultDB
        result == ret
        where:
        resultDB        | ret
        new Database()  | true
        null            | false
    }

    def "Test for exist database NoSuchObjectException"(){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorDatabaseService = new HiveConnectorDatabaseService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )
        when:
        def result = hiveConnectorDatabaseService.exists(connectorContext, QualifiedName.ofDatabase("testhive","testdb"))
        then:
        client.getDatabase(_) >> {throw new NoSuchObjectException()}
        result == false
    }

    def "Test for exist database TException"(){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorDatabaseService = new HiveConnectorDatabaseService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )
        when:
        def result = hiveConnectorDatabaseService.exists(connectorContext, QualifiedName.ofDatabase("testhive","testdb"))
        then:
        client.getDatabase(_) >> {throw new TException() }
        thrown ConnectorException
    }

    @Unroll
    def "Test for listNames database with Exceptions"(){
        def client = Mock(MetacatHiveClient)
        def hiveConnectorDatabaseService = new HiveConnectorDatabaseService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )

        when:
        def dbs = hiveConnectorDatabaseService.listNames(connectorContext, QualifiedName.ofDatabase("testhive","testdb"), null, new Sort(null, SortOrder.ASC), null )
        then:
        1 * client.getAllDatabases() >> {throw exception}
        thrown result

        where:
        exception                    | result
        new TException()             |ConnectorException
        new MetaException()          |InvalidMetaException

    }

    def "Test for get database" (){
        when:
        def dbInfo = hiveConnectorDatabaseService.get( connectorContext, QualifiedName.ofDatabase("testhive", "testdb"))
        then:
        dbInfo.name.catalogName == "testhive"
        dbInfo.name.databaseName == "testdb"
    }

    @Unroll
    def "Test for get database with exceptions" (){
        def client = Mock(MetacatHiveClient);
        def hiveConnectorDatabaseService = new HiveConnectorDatabaseService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )
        when:
        hiveConnectorDatabaseService.get( connectorContext, QualifiedName.ofDatabase("testhive", "testdb2"))
        then:
        1 * client.getDatabase(_) >> { throw exception}
        thrown result
        where:
        exception                    | result
        new NoSuchObjectException()  | DatabaseNotFoundException
        new TException()             | ConnectorException
        new MetaException()          | InvalidMetaException
    }

    def "Test for list database" (){
        when:
        def dbs = hiveConnectorDatabaseService.list( connectorContext, QualifiedName.ofDatabase("testhive", ""), QualifiedName.ofDatabase("testhive", "test"), new Sort(null, SortOrder.ASC), null)
        then:
        def expected = MetacatDataInfoProvider.getAllTestDatabaseInfo()
        dbs == expected
    }

    @Unroll
    def "Test for list database exceptions" (){
        def client = Mock(MetacatHiveClient);
        def hiveConnectorDatabaseService = new HiveConnectorDatabaseService("testhive", client, new HiveConnectorInfoConverter( new HiveTypeConverter() ) )

        when:
        def dbs = hiveConnectorDatabaseService.list( connectorContext, QualifiedName.ofDatabase("testhive", ""), QualifiedName.ofDatabase("testhive", "test"), null, null)
        then:
        1 * client.getAllDatabases() >> {throw exception}
        thrown result
        where:
        exception                    | result
        new TException()             |ConnectorException
        new MetaException()          |InvalidMetaException
    }

    @Unroll
    def "Test for listNames database with page" (){
        given:
        def dbs = hiveConnectorDatabaseService.listNames(
            connectorContext, QualifiedName.ofDatabase("testhive", ""), QualifiedName.ofDatabase("testhive", "test"), new Sort(null, SortOrder.ASC), pageable)

        expect:
        dbs == result

        where:
        pageable          | result
        new Pageable(2,1) |[QualifiedName.ofDatabase("testhive", "test2")]
        new Pageable(1,0) |[QualifiedName.ofDatabase("testhive", "test1")]
        new Pageable(0,0) |[]
    }

}
