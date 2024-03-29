/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.server.properties.Config;
import com.netflix.spectator.api.Registry;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.context.ApplicationContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Connector Config.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@Data
public class ConnectorContext {
    /**
     * Catalog name.
     */
    private final String catalogName;
    /**
     * Catalog shard name.
     */
    private final String catalogShardName;
    /**
     * Connector type.
     */
    private final String connectorType;
    /**
     * Metacat config.
     */
    private final Config config;
    /**
     * The registry for spectator.
     */
    private final Registry registry;
    /**
     * Main application context.
     */
    private final ApplicationContext applicationContext;
    /**
     * Metacat catalog configuration.
     */
    private final Map<String, String> configuration;
    /**
     * Nested connector contexts.
     */
    private final List<ConnectorContext> nestedConnectorContexts;

    /**
     * Default Ctor.
     *
     * @param catalogName        the catalog name.
     * @param catalogShardName   the catalog shard name
     * @param connectorType      the connector type.
     * @param config             the application config.
     * @param registry           the registry.
     * @param applicationContext the application context.
     * @param configuration      the connector properties.
     */
    public ConnectorContext(final String catalogName, final String catalogShardName, final String connectorType,
                            final Config config, final Registry registry,
                            final ApplicationContext applicationContext, final Map<String, String> configuration) {
        this(catalogName, catalogShardName, connectorType, config, registry,
                applicationContext, configuration, Collections.emptyList());
    }
}
