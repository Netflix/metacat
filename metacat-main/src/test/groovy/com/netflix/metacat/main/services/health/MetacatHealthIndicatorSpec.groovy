package com.netflix.metacat.main.services.health

import com.netflix.metacat.common.server.util.ThreadServiceManager
import com.netflix.metacat.main.manager.CatalogManager
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.main.manager.PluginManager
import com.netflix.metacat.main.services.MetacatThriftService
import com.netflix.metacat.main.services.init.MetacatCoreInitService
import com.netflix.metacat.main.services.init.MetacatThriftInitService
import com.netflix.metacat.thrift.CatalogThriftService
import org.springframework.boot.actuate.health.Status
import org.springframework.context.ApplicationContext
import spock.lang.Specification

class MetacatHealthIndicatorSpec extends Specification {
    def pluginManager = Mock(PluginManager)
    def catalogManager = Mock(CatalogManager)
    def connectorManager = Mock(ConnectorManager)
    def threadServiceManager = Mock(ThreadServiceManager)
    def thriftService = Mock(MetacatThriftService)
    def catalogThriftService = Mock(CatalogThriftService)
    def appContext = Mock(ApplicationContext)

    def coreInitService = new MetacatCoreInitService(
        pluginManager,
        catalogManager,
        connectorManager,
        threadServiceManager,
        appContext
    )

    def thriftInitService = new MetacatThriftInitService(
        thriftService, coreInitService
    )

    def healthIndicator = new MetacatHealthIndicator(
        coreInitService, thriftInitService
    )

    def "consider all services for health metrics"() {
        given:
        thriftService.getCatalogThriftServices() >> []
        pluginManager.arePluginsLoaded() >> true
        catalogManager.areCatalogsLoaded() >> true

        thriftInitService.start()

        when:
        def health = healthIndicator.health()

        then:
        health.getStatus() == Status.UP
        health.getDetails().size() == 3
        health.getDetails().get(MetacatHealthIndicator.PLUGIN_KEY) == true
        health.getDetails().get(MetacatHealthIndicator.CATALOG_KEY) == true
        health.getDetails().get(MetacatHealthIndicator.THRIFT_KEY) == true
    }

    def "correctly sets unhealthy when catalogs are not loaded"() {
        when:
        thriftService.getCatalogThriftServices() >> [catalogThriftService]
        catalogThriftService.getPortNumber() >> 77
        pluginManager.arePluginsLoaded() >> true
        catalogManager.areCatalogsLoaded() >> false

        def health = healthIndicator.health()

        then:
        health.getStatus() == Status.OUT_OF_SERVICE
        health.getDetails().get(MetacatHealthIndicator.PLUGIN_KEY) == true
        health.getDetails().get(MetacatHealthIndicator.CATALOG_KEY) == false
        health.getDetails().get(MetacatHealthIndicator.THRIFT_KEY) == false
    }
}
