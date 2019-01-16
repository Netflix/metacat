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

import com.google.common.collect.ImmutableMap
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.connector.hive.client.embedded.EmbeddedHiveClient
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter
import com.netflix.metacat.connector.hive.metastore.HMSHandlerProxy
import com.netflix.metacat.connector.hive.metastore.IMetacatHMSHandler
import com.netflix.metacat.connector.hive.metastore.MetacatHMSHandler
import com.netflix.metacat.connector.hive.util.HiveConfigConstants
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.hadoop.hive.metastore.api.MetaException
import spock.lang.Shared
import spock.lang.Specification

import javax.jdo.JDODataStoreException
import java.lang.reflect.Proxy
import java.util.concurrent.TimeUnit

class HiveConnectorTableIISpec extends Specification {
    @Shared
    MeterRegistry registry = new SimpleMeterRegistry()
    @Shared
    ConnectorRequestContext connectorRequestContext = new ConnectorRequestContext(1, null)
    @Shared
    ConnectorContext connectorContext = new ConnectorContext(
        "testHive",
        "testHive",
        "hive",
        Mock(Config),
        registry,
        ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true")
    )
    @Shared
    HiveConnectorDatabaseService hiveConnectorDatabaseService = Mock(HiveConnectorDatabaseService)

    /*
    def setupSpec() {
    }*/

    def "Test for HMSHandlerProxy handling with empty pool JDODataStoreException"() {
        def hmsHandler = Mock(MetacatHMSHandler)
        def proxy = new HMSHandlerProxy()
        Class[] interfaces = [IMetacatHMSHandler]
        proxy.setMetacatHMSHandler(hmsHandler)
        def Iproxy = (IMetacatHMSHandler) Proxy.newProxyInstance(
            HMSHandlerProxy.class.getClassLoader(),interfaces, proxy);

        def jdoException = new JDODataStoreException("Timeout: Pool empty. Unable to fetch a connection")
        def metaException = new MetaException(jdoException.getMessage())
        metaException.initCause(jdoException)
        def depMockclient = new EmbeddedHiveClient("test", Iproxy, registry)

        def hiveConnectorTableService = new HiveConnectorTableService("testhive", depMockclient, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)
        when:
        hiveConnectorTableService.get(connectorRequestContext,QualifiedName.ofTable("testhive", "test1", "testingtable") )

        then:
        hmsHandler.get_table(_,_) >> { throw metaException}
        thrown ConnectorException
    }

    def "Test for HMSHandlerProxy handle MetaException"() {
        def hmsHandler = Mock(MetacatHMSHandler)
        def proxy = new HMSHandlerProxy()
        Class[] interfaces = [IMetacatHMSHandler]
        proxy.setMetacatHMSHandler(hmsHandler)
        def Iproxy = (IMetacatHMSHandler) Proxy.newProxyInstance(
            HMSHandlerProxy.class.getClassLoader(),interfaces, proxy);

        def metaException = new MetaException("Metastore exception")
        def depMockclient = new EmbeddedHiveClient("test", Iproxy, registry)

        def hiveConnectorTableService = new HiveConnectorTableService("testhive", depMockclient, hiveConnectorDatabaseService, new HiveConnectorInfoConverter(new HiveTypeConverter()), connectorContext)
        when:
        hiveConnectorTableService.get(connectorRequestContext,QualifiedName.ofTable("testhive", "test1", "testingtable") )

        then:
        hmsHandler.get_table(_,_) >> { throw metaException}
        thrown InvalidMetaException
    }
}
