package com.netflix.metacat.common.server.connectors

import com.netflix.metacat.common.server.api.authorization.Authorization
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter
import org.spockframework.util.Nullable
import org.springframework.context.ApplicationContext
import spock.lang.Specification
import spock.lang.Unroll

class ConnectorFactoryDecoratorSpec extends Specification {
    ConnectorPlugin connectorPlugin
    ConnectorFactory delegate
    ConnectorContext connectorContext
    RateLimiter rateLimiter
    Authorization authorization
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
        authorization = Mock(Authorization)
        applicationContext = Mock(ApplicationContext)

        catalogService = Mock(ConnectorCatalogService)
        databaseService = Mock(ConnectorDatabaseService)
        tableService = Mock(ConnectorTableService)
        partitionService = Mock(ConnectorPartitionService)

        connectorPlugin.create(connectorContext) >> delegate
        connectorContext.getApplicationContext() >> applicationContext
        applicationContext.getBean(RateLimiter) >> rateLimiter
        applicationContext.getBean(Authorization) >> authorization

        delegate.getCatalogService() >> catalogService
        delegate.getDatabaseService() >> databaseService
        delegate.getTableService() >> tableService
        delegate.getPartitionService() >> partitionService
    }

    @Unroll
    def "when rate limiting exempted is #rateLimitingExempted and auth exempted is #authExempted"() {
        given:
        connectorContext.getConfiguration() >> ["connector.rate-limiter-exempted": rateLimitingExempted.toString(), "connector.authorization-exempted": authExempted.toString()]
        factory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext)

        when:
        def catalogSvc = factory.getCatalogService()
        def dbSvc = factory.getDatabaseService()
        def tblSvc = factory.getTableService()
        def partitionSvc = factory.getPartitionService()

        then:
        validate(catalogSvc, catalogService, "Catalog", rateLimitingExempted, authExempted)
        validate(dbSvc, databaseService, "Database", rateLimitingExempted, authExempted)
        validate(tblSvc, tableService, "Table", rateLimitingExempted, authExempted)
        validate(partitionSvc, partitionService, "Partition", rateLimitingExempted, authExempted)

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

    void validate(Object svc, Object baseSvc, String svcName, @Nullable Boolean rateLimitingExempted, @Nullable Boolean authExempted) {
        Class throttlingConnectorClass = Class.forName("com.netflix.metacat.common.server.connectors.ThrottlingConnector${svcName}Service")
        Class authEnabledConnectorClass = Class.forName("com.netflix.metacat.common.server.connectors.AuthEnabledConnector${svcName}Service")

        if (rateLimitingExempted == null || !rateLimitingExempted) {
            assert throttlingConnectorClass.isAssignableFrom(svc.getClass()) : "${svcName} service should be ThrottlingConnector${svcName}Service when rate limiting is enabled"
            if (authExempted == null || !authExempted) {
                assert authEnabledConnectorClass.isAssignableFrom(svc.getDelegate().getClass()) : "${svcName} service delegate should be AuthEnabledConnector${svcName}Service when rate limiting and auth are enabled"
            } else {
                assert svc.getDelegate() == baseSvc : "${svcName} service delegate should be base service when when rate limiting is enabled and auth is disabled"
            }
        } else {
            if (authExempted == null || !authExempted) {
                assert authEnabledConnectorClass.isAssignableFrom(svc.getClass()) : "${svcName} service should be AuthEnabledConnector${svcName}Service when rate limiting is disabled and auth is enabled"
                assert svc.getDelegate() == baseSvc : "${svcName} service delegate should be the base service when rate limiting is disabled and auth is enabled"
            } else {
                assert svc == baseSvc : "${svcName} service should be the base service when both rate limiting and throttling are disabled"
            }
        }
    }
}

