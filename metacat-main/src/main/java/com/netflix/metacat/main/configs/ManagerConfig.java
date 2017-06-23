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
package com.netflix.metacat.main.configs;

import com.netflix.metacat.common.server.converter.TypeConverterFactory;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.type.TypeManager;
import com.netflix.metacat.common.type.TypeRegistry;
import com.netflix.metacat.common.server.manager.CatalogManager;
import com.netflix.metacat.common.server.connectors.ConnectorManager;
import com.netflix.metacat.common.server.manager.PluginManager;
import com.netflix.spectator.api.Registry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for Management beans.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
public class ManagerConfig {

    /**
     * Manager of the connectors.
     *
     * @param config System config
     * @return The connector manager instance to use.
     */
    @Bean
    public ConnectorManager connectorManager(final Config config) {
        return new ConnectorManager(config);
    }

    /**
     * Type manager to use.
     *
     * @return The type registry
     */
    @Bean
    public TypeManager typeManager() {
        // TODO: Get rid of this static instantiation as Spring will manage singleton
        return TypeRegistry.getTypeRegistry();
    }

    /**
     * The plugin manager.
     *
     * @param connectorManager     Connector manager to use
     * @param typeConverterFactory Type converter factory to use
     * @return The plugin manager instance
     */
    @Bean
    public PluginManager pluginManager(
        final ConnectorManager connectorManager,
        final TypeConverterFactory typeConverterFactory
    ) {
        return new PluginManager(connectorManager, typeConverterFactory);
    }

    /**
     * Catalog manager.
     *
     * @param connectorManager The connector manager to use
     * @param config           The system configuration to use
     * @param registry         registry for spectator
     * @return Configured catalog manager
     */
    @Bean
    public CatalogManager catalogManager(
        final ConnectorManager connectorManager,
        final Config config,
        final Registry registry
    ) {
        return new CatalogManager(connectorManager, config, registry);
    }
}
