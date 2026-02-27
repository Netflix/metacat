/*
 *
 *  Copyright 2024 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter
import org.springframework.context.ApplicationContext
import spock.lang.Specification
import spock.lang.Unroll

class ConnectorFactoryDecoratorSpec extends Specification {
    ConnectorPlugin connectorPlugin
    ConnectorFactory delegate
    ConnectorContext connectorContext
    RateLimiter rateLimiter
    ApplicationContext applicationContext
    ConnectorCatalogService catalogService
    ConnectorDatabaseService databaseService
    ConnectorTableService tableService
    ConnectorPartitionService partitionService

    def factory

    def setup() {
        connectorPlugin = Mock(ConnectorPlugin)
        delegate = Mock(ConnectorFactory)
        connectorContext = Mock(ConnectorContext)
        rateLimiter = Mock(RateLimiter)
        applicationContext = Mock(ApplicationContext)

        catalogService = Mock(ConnectorCatalogService)
        databaseService = Mock(ConnectorDatabaseService)
        tableService = Mock(ConnectorTableService)
        partitionService = Mock(ConnectorPartitionService)

        connectorPlugin.create(connectorContext) >> delegate
        connectorContext.getApplicationContext() >> applicationContext
        applicationContext.getBean(RateLimiter) >> rateLimiter

        delegate.getCatalogService() >> catalogService
        delegate.getDatabaseService() >> databaseService
        delegate.getTableService() >> tableService
        delegate.getPartitionService() >> partitionService
    }

    def "when rate limiting is enabled"() {
        given:
        connectorContext.getConfiguration() >> ["connector.rate-limiter-exempted": "false"]
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def catalogSvc = factory.getCatalogService()

        then:
        catalogSvc instanceof ThrottlingConnectorCatalogService
        (catalogSvc as ThrottlingConnectorCatalogService).getDelegate() == catalogService

        when:
        def dbSvc = factory.getDatabaseService()

        then:
        dbSvc instanceof ThrottlingConnectorDatabaseService
        (dbSvc as ThrottlingConnectorDatabaseService).getDelegate() == databaseService

        when:
        def tblSvc = factory.getTableService()

        then:
        tblSvc instanceof ThrottlingConnectorTableService
        (tblSvc as ThrottlingConnectorTableService).getDelegate() == tableService

        when:
        def partitionSvc = factory.getPartitionService()

        then:
        partitionSvc instanceof ThrottlingConnectorPartitionService
        (partitionSvc as ThrottlingConnectorPartitionService).getDelegate() == partitionService
    }

    @Unroll
    def "when rate limiting is disabled"() {
        given:
        connectorContext.getConfiguration() >> config
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def catalogSvc = factory.getCatalogService()

        then:
        catalogSvc == catalogService

        when:
        def dbSvc = factory.getDatabaseService()

        then:
        dbSvc == databaseService

        when:
        def tblSvc = factory.getTableService()

        then:
        tblSvc == tableService

        when:
        def partitionSvc = factory.getPartitionService()

        then:
        partitionSvc == partitionService

        where:
        config                                      | ignored
        ["connector.rate-limiter-exempted": "true"] | null
        null                                        | null
    }

    def "when authorization is enabled with rate limiting"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "false",
            "connector.authorization-required": "true",
            "connector.authorized-callers": "irc,irc-server"
        ]
        connectorContext.getCatalogName() >> "ads"
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def catalogSvc = factory.getCatalogService()

        then: "outer layer is authorization"
        catalogSvc instanceof AuthorizingConnectorCatalogService
        def authCatalogSvc = catalogSvc as AuthorizingConnectorCatalogService

        and: "inner layer is throttling"
        authCatalogSvc.getDelegate() instanceof ThrottlingConnectorCatalogService
        def throttleCatalogSvc = authCatalogSvc.getDelegate() as ThrottlingConnectorCatalogService

        and: "innermost is the actual service"
        throttleCatalogSvc.getDelegate() == catalogService

        when:
        def dbSvc = factory.getDatabaseService()

        then: "outer layer is authorization"
        dbSvc instanceof AuthorizingConnectorDatabaseService
        def authDbSvc = dbSvc as AuthorizingConnectorDatabaseService

        and: "inner layer is throttling"
        authDbSvc.getDelegate() instanceof ThrottlingConnectorDatabaseService
        def throttleDbSvc = authDbSvc.getDelegate() as ThrottlingConnectorDatabaseService

        and: "innermost is the actual service"
        throttleDbSvc.getDelegate() == databaseService

        when:
        def tblSvc = factory.getTableService()

        then: "outer layer is authorization"
        tblSvc instanceof AuthorizingConnectorTableService
        def authTblSvc = tblSvc as AuthorizingConnectorTableService

        and: "inner layer is throttling"
        authTblSvc.getDelegate() instanceof ThrottlingConnectorTableService
        def throttleTblSvc = authTblSvc.getDelegate() as ThrottlingConnectorTableService

        and: "innermost is the actual service"
        throttleTblSvc.getDelegate() == tableService

        when:
        def partitionSvc = factory.getPartitionService()

        then: "outer layer is authorization"
        partitionSvc instanceof AuthorizingConnectorPartitionService
        def authPartitionSvc = partitionSvc as AuthorizingConnectorPartitionService

        and: "inner layer is throttling"
        authPartitionSvc.getDelegate() instanceof ThrottlingConnectorPartitionService
        def throttlePartitionSvc = authPartitionSvc.getDelegate() as ThrottlingConnectorPartitionService

        and: "innermost is the actual service"
        throttlePartitionSvc.getDelegate() == partitionService
    }

    def "when only authorization is enabled (rate limiting disabled)"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "true",
            "connector.authorization-required": "true",
            "connector.authorized-callers": "irc"
        ]
        connectorContext.getCatalogName() >> "ads"
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def catalogSvc = factory.getCatalogService()

        then: "only authorization layer present"
        catalogSvc instanceof AuthorizingConnectorCatalogService
        def authCatalogSvc = catalogSvc as AuthorizingConnectorCatalogService

        and: "directly wraps the actual service"
        authCatalogSvc.getDelegate() == catalogService

        when:
        def dbSvc = factory.getDatabaseService()

        then: "only authorization layer present"
        dbSvc instanceof AuthorizingConnectorDatabaseService
        def authDbSvc = dbSvc as AuthorizingConnectorDatabaseService

        and: "directly wraps the actual service"
        authDbSvc.getDelegate() == databaseService

        when:
        def tblSvc = factory.getTableService()

        then: "only authorization layer present"
        tblSvc instanceof AuthorizingConnectorTableService
        def authTblSvc = tblSvc as AuthorizingConnectorTableService

        and: "directly wraps the actual service"
        authTblSvc.getDelegate() == tableService

        when:
        def partitionSvc = factory.getPartitionService()

        then: "only authorization layer present"
        partitionSvc instanceof AuthorizingConnectorPartitionService
        def authPartitionSvc = partitionSvc as AuthorizingConnectorPartitionService

        and: "directly wraps the actual service"
        authPartitionSvc.getDelegate() == partitionService
    }

    def "authorization applied to all services including partition and catalog"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "false",
            "connector.authorization-required": "true",
            "connector.authorized-callers": "irc"
        ]
        connectorContext.getCatalogName() >> "ads"
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def catalogSvc = factory.getCatalogService()
        def partitionSvc = factory.getPartitionService()

        then: "catalog service has authorization"
        catalogSvc instanceof AuthorizingConnectorCatalogService

        and: "partition service has authorization"
        partitionSvc instanceof AuthorizingConnectorPartitionService
    }

    def "empty authorized callers list"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "true",
            "connector.authorization-required": "true",
            "connector.authorized-callers": ""
        ]
        connectorContext.getCatalogName() >> "ads"
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def tblSvc = factory.getTableService()

        then: "authorization still applied with empty callers set"
        tblSvc instanceof AuthorizingConnectorTableService
    }

    def "authorization disabled by default"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "true"
            // authorization-required not set
        ]
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def tblSvc = factory.getTableService()

        then: "no authorization wrapper"
        tblSvc == tableService
    }

    def "authorized callers list trims whitespace"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "true",
            "connector.authorization-required": "true",
            "connector.authorized-callers": "  irc  ,  irc-server  ,spark"
        ]
        connectorContext.getCatalogName() >> "ads"
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def tblSvc = factory.getTableService()

        then: "authorization is applied"
        tblSvc instanceof AuthorizingConnectorTableService
        // The whitespace should be trimmed, so "irc", "irc-server", and "spark" should all be valid
    }

    def "authorized callers with only whitespace entries are filtered"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "true",
            "connector.authorization-required": "true",
            "connector.authorized-callers": "irc,   ,,"
        ]
        connectorContext.getCatalogName() >> "ads"
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def tblSvc = factory.getTableService()

        then: "authorization is applied with only non-empty callers"
        tblSvc instanceof AuthorizingConnectorTableService
    }

    def "location update lock enabled alone wraps only table service"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "true",
            "connector.location-update-locked": "true"
        ]
        connectorContext.getCatalogName() >> "ads"
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def tblSvc = factory.getTableService()

        then: "table service is wrapped with AuthorizingConnectorTableService"
        tblSvc instanceof AuthorizingConnectorTableService
        def authTblSvc = tblSvc as AuthorizingConnectorTableService
        authTblSvc.getDelegate() == tableService

        when:
        def catalogSvc = factory.getCatalogService()

        then: "catalog service is NOT wrapped"
        catalogSvc == catalogService

        when:
        def dbSvc = factory.getDatabaseService()

        then: "database service is NOT wrapped"
        dbSvc == databaseService

        when:
        def partitionSvc = factory.getPartitionService()

        then: "partition service is NOT wrapped"
        partitionSvc == partitionService
    }

    def "location update lock and auth both enabled wraps all services"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "true",
            "connector.authorization-required": "true",
            "connector.authorized-callers": "irc",
            "connector.location-update-locked": "true"
        ]
        connectorContext.getCatalogName() >> "ads"
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def tblSvc = factory.getTableService()

        then: "table service is wrapped"
        tblSvc instanceof AuthorizingConnectorTableService

        when:
        def catalogSvc = factory.getCatalogService()

        then: "catalog service is wrapped (via auth)"
        catalogSvc instanceof AuthorizingConnectorCatalogService

        when:
        def dbSvc = factory.getDatabaseService()

        then: "database service is wrapped (via auth)"
        dbSvc instanceof AuthorizingConnectorDatabaseService

        when:
        def partitionSvc = factory.getPartitionService()

        then: "partition service is wrapped (via auth)"
        partitionSvc instanceof AuthorizingConnectorPartitionService
    }

    def "location update lock disabled by default when config key absent"() {
        given:
        connectorContext.getConfiguration() >> [
            "connector.rate-limiter-exempted": "true"
            // location-update-locked not set
        ]
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def tblSvc = factory.getTableService()

        then: "no wrapper applied"
        tblSvc == tableService
    }
}
