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
package com.netflix.metacat.main.manager

import com.google.common.collect.ImmutableMap
import com.google.common.collect.Maps
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig
import com.netflix.spectator.api.Registry
import org.assertj.core.util.Sets
import org.springframework.boot.actuate.health.Status
import spock.lang.Specification

/**
 * Specifications for the CatalogManager class.
 *
 * @author tgianos
 * @since 1.1.0
 */
class CatalogManagerSpec extends Specification {
    def "can report health accurately based on whether catalogs are loaded or not"() {
        def connectorManager = Mock(ConnectorManager) {
            2 * getCatalogs() >>> [
                [],
                [
                    MetacatCatalogConfig.createFromMapAndRemoveProperties("hive", "testhive", Maps.newHashMap()),
                    MetacatCatalogConfig.createFromMapAndRemoveProperties("hive", "prodhive", Maps.newHashMap()),
                    MetacatCatalogConfig.createFromMapAndRemoveProperties("mysql", "finance", Maps.newHashMap())
                ]
            ]
        }
        def config = Mock(Config) {
            1 * getPluginConfigLocation() >> File.createTempFile("not", ".aDirectory").getAbsolutePath()
        }
        def registry = Mock(Registry)
        def catalogManager = new CatalogManager(connectorManager, config, registry)

        when:
        def health = catalogManager.health()

        then:
        health.getStatus() == Status.OUT_OF_SERVICE
        health.getDetails().size() == 0

        when:
        catalogManager.loadCatalogs()
        health = catalogManager.health()

        then:
        health.getStatus() == Status.UP
        health.getDetails().size() == 3
        health.getDetails().get("testhive") == "hive"
        health.getDetails().get("prodhive") == "hive"
        health.getDetails().get("finance") == "mysql"
    }
}
