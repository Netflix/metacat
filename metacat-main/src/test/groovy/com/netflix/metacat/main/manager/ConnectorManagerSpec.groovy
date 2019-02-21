/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package com.netflix.metacat.main.manager

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService
import com.netflix.metacat.common.server.connectors.ConnectorFactory
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService
import com.netflix.metacat.common.server.connectors.ConnectorPlugin
import com.netflix.metacat.common.server.connectors.ConnectorTableService
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter
import com.netflix.metacat.common.server.properties.Config
import spock.lang.Specification

/**
 * Specification for ConnectorManager.
 *
 * @author amajumdar
 * @since 1.2.0
 */
class ConnectorManagerSpec extends Specification {
    def config = Mock(Config)
    def plugin = Mock(ConnectorPlugin)

    def setup() {
        plugin.create(_) >> Mock(ConnectorFactory)
        plugin.getInfoConverter() >> Mock(ConnectorInfoConverter)
        plugin.getType() >> 'default'
        plugin.getTypeConverter() >> Mock(ConnectorTypeConverter)
    }

    def testAddPlugin(){
        given:
        def connectorManager = new ConnectorManager(config)
        when:
        connectorManager.addPlugin(plugin)
        then:
        noExceptionThrown()
    }

    def testCreateConnection(){
        given:
        def connectorManager = new ConnectorManager(config)
        def context = Mock(ConnectorContext)
        connectorManager.addPlugin(plugin)
        def hivePlugin = Mock(ConnectorPlugin)
        def hiveConnectorFactory = Mock(ConnectorFactory)
        def databaseService = Mock(ConnectorDatabaseService)
        def tableService = Mock(ConnectorTableService)
        def partitionService = Mock(ConnectorPartitionService)
        hiveConnectorFactory.getDatabaseService() >> databaseService
        hiveConnectorFactory.getTableService() >> tableService
        hiveConnectorFactory.getPartitionService() >> partitionService
        def hiveConnectorFactory1 = Mock(ConnectorFactory)
        def databaseService1 = Mock(ConnectorDatabaseService)
        def tableService1 = Mock(ConnectorTableService)
        def partitionService1 = Mock(ConnectorPartitionService)
        hiveConnectorFactory1.getDatabaseService() >> databaseService1
        hiveConnectorFactory1.getTableService() >> tableService1
        hiveConnectorFactory1.getPartitionService() >> partitionService1
        def hiveConnectorFactory2 = Mock(ConnectorFactory)
        def databaseService2 = Mock(ConnectorDatabaseService)
        def tableService2 = Mock(ConnectorTableService)
        def partitionService2 = Mock(ConnectorPartitionService)
        hiveConnectorFactory2.getDatabaseService() >> databaseService2
        hiveConnectorFactory2.getTableService() >> tableService2
        hiveConnectorFactory2.getPartitionService() >> partitionService2
        hivePlugin.create(_) >>> [hiveConnectorFactory, hiveConnectorFactory1, hiveConnectorFactory2]
        def connectorInfoConverter = Mock(ConnectorInfoConverter)
        def connectorTypeConverter = Mock(ConnectorTypeConverter)
        hivePlugin.getInfoConverter() >> connectorInfoConverter
        hivePlugin.getType() >> 'hive'
        hivePlugin.getTypeConverter() >> connectorTypeConverter
        connectorManager.addPlugin(hivePlugin)
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'invalid'
        0 * context.getConfiguration()
        noExceptionThrown()
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'default'
        1 * context.getCatalogName() >> 'a'
        1 * context.getConfiguration() >> [:]
        noExceptionThrown()
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'hive'
        1 * context.getCatalogName() >> 'h'
        1 * context.getConfiguration() >> [:]
        noExceptionThrown()
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'default'
        1 * context.getCatalogName() >> 'a'
        1 * context.getConfiguration() >> [:]
        thrown(IllegalStateException)
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'default'
        1 * context.getCatalogName() >> 'a1'
        1 * context.getConfiguration() >> [:]
        noExceptionThrown()
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'default'
        1 * context.getCatalogName() >> 'a'
        1 * context.getConfiguration() >> ['metacat.schema.whitelist':'d1']
        noExceptionThrown()
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'default'
        1 * context.getCatalogName() >> 'a'
        1 * context.getConfiguration() >> ['metacat.schema.whitelist':'d1']
        thrown(IllegalStateException)
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'default'
        1 * context.getCatalogName() >> 'a'
        1 * context.getConfiguration() >> ['metacat.schema.whitelist':'d2,d3']
        noExceptionThrown()
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'hive'
        1 * context.getCatalogName() >> 'h'
        1 * context.getConfiguration() >> ['metacat.schema.whitelist':'d1,d2']
        noExceptionThrown()
        when:
        connectorManager.createConnection(context)
        then:
        1 * context.getConnectorType() >> 'hive'
        1 * context.getCatalogName() >> 'h1'
        1 * context.getConfiguration() >> [:]
        noExceptionThrown()
        expect:
        connectorManager.getCatalogConfigs().size() == 7
        connectorManager.getTypeConverter('hive') == connectorTypeConverter
        connectorManager.getInfoConverter('hive') == connectorInfoConverter
        connectorManager.getDatabaseService(QualifiedName.fromString('h/d1')) == databaseService1
        connectorManager.getTableService(QualifiedName.fromString('h/d1')) == tableService1
        connectorManager.getPartitionService(QualifiedName.fromString('h/d1')) == partitionService1
        connectorManager.getDatabaseServices().size() == 4
        connectorManager.getTableServices().size() == 4
        connectorManager.getPartitionServices().size() == 4
    }
}
