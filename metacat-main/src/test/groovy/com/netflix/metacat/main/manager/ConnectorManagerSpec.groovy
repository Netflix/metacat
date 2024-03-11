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
import com.netflix.metacat.common.server.api.authorization.Authorization
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter
import com.netflix.metacat.common.server.connectors.ConnectorCatalogService
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService
import com.netflix.metacat.common.server.connectors.ConnectorFactory
import com.netflix.metacat.common.server.connectors.ConnectorFactoryDecorator
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService
import com.netflix.metacat.common.server.connectors.ConnectorPlugin
import com.netflix.metacat.common.server.connectors.ConnectorTableService
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter
import com.netflix.metacat.common.server.properties.Config
import org.springframework.context.ApplicationContext
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
    def rateLimiter = Mock(RateLimiter)
    def authorization = Mock(Authorization)
    def context = Mock(ConnectorContext)
    def appContext = Mock(ApplicationContext)

    def setup() {
        plugin.create(_ as ConnectorContext) >> Mock(ConnectorFactory)
        plugin.getInfoConverter() >> Mock(ConnectorInfoConverter)
        plugin.getType() >> 'default'
        plugin.getTypeConverter() >> Mock(ConnectorTypeConverter)

        context.getApplicationContext() >> appContext
        appContext.getBean(RateLimiter) >> rateLimiter
        appContext.getBean(Authorization) >> authorization
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
        hivePlugin.create(_ as ConnectorContext) >>> [hiveConnectorFactory, hiveConnectorFactory1, hiveConnectorFactory2]

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
        0 * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true"]
        noExceptionThrown()

        when:
        connectorManager.createConnection(context)

        then:
        (1.._) * context.getConnectorType() >> 'default'
        (1.._) * context.getCatalogName() >> 'a'
        (1.._) * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true"]
        noExceptionThrown()

        when:
        connectorManager.createConnection(context)

        then:
        (1.._) * context.getConnectorType() >> 'hive'
        (1.._) * context.getCatalogName() >> 'h'
        (1.._) * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true"]
        noExceptionThrown()

        when:
        connectorManager.createConnection(context)

        then:
        (1.._) * context.getConnectorType() >> 'default'
        (1.._) * context.getCatalogName() >> 'a'
        (1.._) * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true"]
        thrown(IllegalStateException)

        when:
        connectorManager.createConnection(context)

        then:
        (1.._) * context.getConnectorType() >> 'default'
        (1.._) * context.getCatalogName() >> 'a1'
        (1.._) * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true"]
        noExceptionThrown()

        when:
        connectorManager.createConnection(context)

        then:
        (1.._) * context.getConnectorType() >> 'default'
        (1.._) * context.getCatalogName() >> 'a'
        (1.._) * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true", 'metacat.schema.whitelist':'d1']
        noExceptionThrown()

        when:
        connectorManager.createConnection(context)

        then:
        (1.._) * context.getConnectorType() >> 'default'
        (1.._) * context.getCatalogName() >> 'a'
        (1.._) * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true", 'metacat.schema.whitelist':'d1']
        thrown(IllegalStateException)

        when:
        connectorManager.createConnection(context)

        then:
        (1.._) * context.getConnectorType() >> 'default'
        (1.._) * context.getCatalogName() >> 'a'
        (1.._) * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true", 'metacat.schema.whitelist':'d2,d3']
        noExceptionThrown()

        when:
        connectorManager.createConnection(context)

        then:
        (1.._) * context.getConnectorType() >> 'hive'
        (1.._) * context.getCatalogName() >> 'h'
        (1.._) * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true", 'metacat.schema.whitelist':'d1,d2']
        noExceptionThrown()

        when:
        connectorManager.createConnection(context)

        then:
        (1.._) * context.getConnectorType() >> 'hive'
        (1.._) * context.getCatalogName() >> 'h1'
        (1.._) * context.getConfiguration() >> ["connector.rate-limiter-exempted": "true", "connector.authorization-exempted": "true"]
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

    def "instantiates validating (throttling and/or auth enabled) or base connector correctly"() {
        given:
        def connectorManager = new ConnectorManager(config)
        def connectorInfoConverter = Mock(ConnectorInfoConverter)
        def connectorTypeConverter = Mock(ConnectorTypeConverter)
        def hivePlugin = Mock(ConnectorPlugin)
        def hiveConnectorFactory = Mock(ConnectorFactory)
        def catalogService = Mock(ConnectorCatalogService)
        def databaseService = Mock(ConnectorDatabaseService)
        def tableService = Mock(ConnectorTableService)
        def partitionService = Mock(ConnectorPartitionService)

        def configuration = [:]
        if (rateLimitingExempted != null) {
            configuration['connector.rate-limiter-exempted'] = rateLimitingExempted.toString()
        }
        if (authExempted != null) {
            configuration['connector.authorization-exempted'] = authExempted.toString()
        }
        context.getConfiguration() >> configuration
        context.getCatalogName() >> 'h'
        context.getConnectorType() >> "hive"

        hivePlugin.create(context) >> hiveConnectorFactory
        hivePlugin.getInfoConverter() >> connectorInfoConverter
        hivePlugin.getType() >> 'hive'
        hivePlugin.getTypeConverter() >> connectorTypeConverter

        hiveConnectorFactory.getCatalogService() >> catalogService
        hiveConnectorFactory.getDatabaseService() >> databaseService
        hiveConnectorFactory.getTableService() >> tableService
        hiveConnectorFactory.getPartitionService() >> partitionService

        connectorManager.addPlugin(hivePlugin)

        when:
        connectorManager.createConnection(context)

        then:
        noExceptionThrown()
        connectorManager.getCatalogHolder(QualifiedName.fromString("h")).connectorFactory instanceof ConnectorFactoryDecorator
        (connectorManager.getCatalogHolder(QualifiedName.fromString("h")).connectorFactory as ConnectorFactoryDecorator).getDelegate() == hiveConnectorFactory

        where:
        rateLimitingExempted || authExempted
        false                || true
        false                || false
        true                 || true
        true                 || false
        null                 || true
        null                 || false
        null                 || null
        false                || null
        true                 || null
    }
}
