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
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import com.netflix.metacat.common.server.exception.ConnectorException
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.metastoreclient.thrift.MetacatHiveClient
import com.netflix.metacat.testdata.provider.MetacatDataInfoProvider
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.thrift.TException
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Unit test for HiveConnectorDatabaseSpec.
 * @author zhenl
 */
class HiveConnectorDatabaseSpec extends Specification{
    @Shared
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient);
    @Shared
    HiveConnectorDatabaseService hiveConnectorDatabaseService = new HiveConnectorDatabaseService("testhive", this.metacatHiveClient, new HiveConnectorInfoConverter() )
    @Shared
    ConnectorContext connectorContext = new ConnectorContext(1, null);

    def setupSpec() {
        metacatHiveClient.getAllDatabases() >> ["test1", "test2", "dev1", "dev2"]
        metacatHiveClient.getDatabase("testdb2") >> { throw new TException() }
        metacatHiveClient.createDatabase(new Database("testdb3","testdb3","file://temp/",Collections.EMPTY_MAP)) >> {}
        metacatHiveClient.createDatabase(new Database("testdb2","testdb2","file://temp/",Collections.EMPTY_MAP)) >> { throw new TException()}
    }

    def "Test for create database" (){
        when:
            hiveConnectorDatabaseService.create( connectorContext, DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "testdb3")).build())
        then:
        noExceptionThrown()
    }

    def "Test for create database with exception" (){
        when:
        hiveConnectorDatabaseService.create( connectorContext, DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "testdb2")).uri("file://temp/").build())
        then:
        thrown ConnectorException
    }

    def "Test for listNames database"(){
        when:
        def dbs = hiveConnectorDatabaseService.listNames(connectorContext, QualifiedName.ofDatabase("testhive","testdb"), null, null, null )
        then:
        dbs == MetacatDataInfoProvider.getAllDatabaseNames()

    }

    def "Test for get database" (){
        when:
        def dbInfo = hiveConnectorDatabaseService.get( connectorContext, QualifiedName.ofDatabase("testhive", "testdb"))
        then:
        dbInfo.name.catalogName == "testhive"
        dbInfo.name.databaseName == "testdb"
    }

    def "Test for get database with exception" (){
        when:
        hiveConnectorDatabaseService.get( connectorContext, QualifiedName.ofDatabase("testhive", "testdb2"))
        then:
        thrown ConnectorException
    }

    def "Test for list database" (){
        when:
        def dbs = hiveConnectorDatabaseService.list( connectorContext, QualifiedName.ofDatabase("testhive", ""), QualifiedName.ofDatabase("testhive", "test"), null, null)
        then:
        def expected = MetacatDataInfoProvider.getAllTestDatabaseInfo()
        dbs == expected
    }

    @Unroll
    def "Test for listNames database with page" (){
        given:
        def dbs = hiveConnectorDatabaseService.listNames(
            connectorContext, QualifiedName.ofDatabase("testhive", ""), QualifiedName.ofDatabase("testhive", "test"), null, pageable)

        expect:
        dbs == result

        where:
        pageable          | result
        new Pageable(2,1) |[QualifiedName.ofDatabase("testhive", "test2")]
        new Pageable(1,0) |[QualifiedName.ofDatabase("testhive", "test1")]
        new Pageable(0,0) |[]
    }

}
