/*
 *
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
 *
 */
package com.netflix.metacat.main.services

import com.netflix.metacat.common.server.util.ThreadServiceManager
import com.netflix.metacat.main.manager.CatalogManager
import com.netflix.metacat.main.manager.ConnectorManager
import com.netflix.metacat.main.manager.PluginManager
import org.springframework.boot.actuate.health.Status
import org.springframework.context.event.ContextRefreshedEvent
import spock.lang.Specification

/**
 * Specifications for the Initialization service.
 *
 * @author tgianos
 * @since 1.1.0
 */
class MetacatInitializationServiceSpec extends Specification {

    def "can start and stop services"() {
        def pluginManager = Mock(PluginManager)
        def catalogManager = Mock(CatalogManager)
        def connectorManager = Mock(ConnectorManager)
        def threadServiceManager = Mock(ThreadServiceManager)
        def thriftService = Mock(MetacatThriftService)

        def initializationService = new MetacatInitializationService(
            pluginManager,
            catalogManager,
            connectorManager,
            threadServiceManager,
            thriftService
        )

        when:
        def health = initializationService.health()

        then:
        health.getStatus() == Status.OUT_OF_SERVICE
        health.getDetails().size() == 3
        health.getDetails().get(MetacatInitializationService.PLUGIN_KEY) == false
        health.getDetails().get(MetacatInitializationService.CATALOG_KEY) == false
        health.getDetails().get(MetacatInitializationService.THRIFT_KEY) == false

        when:
        initializationService.start(Mock(ContextRefreshedEvent))
        health = initializationService.health()

        then:
        health.getStatus() == Status.UP
        health.getDetails().size() == 3
        health.getDetails().get(MetacatInitializationService.PLUGIN_KEY) == true
        health.getDetails().get(MetacatInitializationService.CATALOG_KEY) == true
        health.getDetails().get(MetacatInitializationService.THRIFT_KEY) == true
    }

    def "can't start services on exception"() {
        def pluginManager = Mock(PluginManager)
        def catalogManager = Mock(CatalogManager)
        def connectorManager = Mock(ConnectorManager)
        def threadServiceManager = Mock(ThreadServiceManager)
        def thriftService = Mock(MetacatThriftService) {
            1 * start() >> { throw new RuntimeException("uh oh") }
        }

        def initializationService = new MetacatInitializationService(
            pluginManager,
            catalogManager,
            connectorManager,
            threadServiceManager,
            thriftService
        )

        when:
        def health = initializationService.health()

        then:
        health.getStatus() == Status.OUT_OF_SERVICE
        health.getDetails().size() == 3
        health.getDetails().get(MetacatInitializationService.PLUGIN_KEY) == false
        health.getDetails().get(MetacatInitializationService.CATALOG_KEY) == false
        health.getDetails().get(MetacatInitializationService.THRIFT_KEY) == false

        when:
        try {
            initializationService.start(Mock(ContextRefreshedEvent))
            assert false
        } catch (final Exception e) {
            assert e != null
        }
        health = initializationService.health()

        then:
        health.getStatus() == Status.OUT_OF_SERVICE
        health.getDetails().size() == 3
        health.getDetails().get(MetacatInitializationService.PLUGIN_KEY) == true
        health.getDetails().get(MetacatInitializationService.CATALOG_KEY) == true
        health.getDetails().get(MetacatInitializationService.THRIFT_KEY) == false
    }
}
