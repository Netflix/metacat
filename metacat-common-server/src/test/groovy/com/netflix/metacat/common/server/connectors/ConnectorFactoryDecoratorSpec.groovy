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
}

