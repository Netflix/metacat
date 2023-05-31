package com.netflix.metacat.main.services.init

import com.netflix.metacat.common.server.util.ThreadServiceManager
import com.netflix.metacat.main.manager.CatalogManager
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.main.manager.PluginManager
import org.springframework.context.ApplicationContext
import spock.lang.Specification

class MetacatCoreInitServiceSpec extends Specification {
    def "can start and stop services correctly"() {
        def pluginManager = Mock(PluginManager)
        def catalogManager = Mock(CatalogManager)
        def connectorManager = Mock(ConnectorManager)
        def threadServiceManager = Mock(ThreadServiceManager)
        def appContext = Mock(ApplicationContext)

        def initializationService = new MetacatCoreInitService(
            pluginManager,
            catalogManager,
            connectorManager,
            threadServiceManager,
            appContext
        )

        when:
        initializationService.start()

        then:
        1 * pluginManager.loadPlugins()
        1 * catalogManager.loadCatalogs(appContext)

        when:
        initializationService.stop()

        then:
        1 * connectorManager.stop()
        1 * threadServiceManager.stop()
    }
}
