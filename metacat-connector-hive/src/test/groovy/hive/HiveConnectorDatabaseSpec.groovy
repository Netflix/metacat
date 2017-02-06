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

package hive

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService
import com.netflix.metacat.connector.hive.MetacatHiveClient
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Created by zhenl on 1/31/17.
 */
class HiveConnectorDatabaseSpec extends Specification{
    @Shared
    MetacatHiveClient metacatHiveClient = Mock(MetacatHiveClient);
    @Shared
    HiveConnectorDatabaseService hiveConnectorDatabaseService = new HiveConnectorDatabaseService(metacatHiveClient, new HiveConnectorInfoConverter() )
    @Shared
    ConnectorContext connectorContext = new ConnectorContext(1, null);

    def setupSpec() {
        metacatHiveClient.getAllDatabases() >> ["test1", "test2"]
    }

    @Unroll
    def "Test for getalldatabases"(){
        given:
        def dbs = hiveConnectorDatabaseService.listNames(connectorContext, QualifiedName.ofDatabase("testhive","testdb"), null, null, null );

        print dbs
        expect:
            dbs == [ QualifiedName.ofDatabase("testhive", "test1") , QualifiedName.ofDatabase("testhive", "test2")]

    }

    @Unroll
    def "Test for get" (){
        given:
        def dbs = hiveConnectorDatabaseService.get( connectorContext, QualifiedName.ofDatabase("testhive", "testdb"))
    }
}
