/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.metacat.common.server.connectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.CatalogNotFoundException;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connector manager.
 */
@Slf4j
public class ConnectorManager {
    // Map of connector plugins registered.
    private final ConcurrentMap<String, ConnectorPlugin> plugins = new ConcurrentHashMap<>();
    // Map of connector factories registered.
    private final ConcurrentMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();
    // Map of catalogs registered.
    private final ConcurrentHashMap<String, MetacatCatalogConfig> catalogs = new ConcurrentHashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Config config;

    /**
     * Constructor.
     *
     * @param config System configuration
     */
    public ConnectorManager(final Config config) {
        this.config = config;
    }

    /**
     * Stop.
     */
    @PreDestroy
    public void stop() {
        if (stopped.getAndSet(true)) {
            return;
        }

        connectorFactories.values().forEach(connectorFactory -> {
            try {
                connectorFactory.stop();
            } catch (Throwable t) {
                log.error(String.format("Error shutting down connector: %s", connectorFactory.getName()), t);
            }
        });
    }

    /**
     * add Plugin.
     *
     * @param connectorPlugin connector plugin
     */
    public void addPlugin(final ConnectorPlugin connectorPlugin) {
        plugins.put(connectorPlugin.getType(), connectorPlugin);
    }

    /**
     * Creates a connection for the given catalog.
     *
     * @param catalogName      catalog name
     * @param connectorType    connector type
     * @param connectorContext metacat connector properties
     */
    public synchronized void createConnection(final String catalogName,
                                              final String connectorType,
                                              final ConnectorContext connectorContext) {
        Preconditions.checkState(!stopped.get(), "ConnectorManager is stopped");
        final ConnectorPlugin connectorPlugin = plugins.get(connectorType);
        if (connectorPlugin != null) {
            Preconditions
                .checkState(!connectorFactories.containsKey(catalogName), "A connector %s already exists", catalogName);
            final ConnectorFactory connectorFactory =
                connectorPlugin.create(catalogName, connectorContext);
            connectorFactories.put(catalogName, connectorFactory);

            final MetacatCatalogConfig catalogConfig =
                MetacatCatalogConfig.createFromMapAndRemoveProperties(connectorType,
                    connectorContext.getConfiguration());
            catalogs.put(catalogName, catalogConfig);
        } else {
            log.warn("No plugin for connector with type %s", connectorType);
        }
    }

    /**
     * Returns the catalog config.
     *
     * @param name name
     * @return catalog config
     */
    @Nonnull
    public MetacatCatalogConfig getCatalogConfig(final QualifiedName name) {
        return getCatalogConfig(name.getCatalogName());
    }

    /**
     * Returns the catalog config.
     *
     * @param catalogName catalog name
     * @return catalog config
     */
    @Nonnull
    public MetacatCatalogConfig getCatalogConfig(final String catalogName) {
        if (Strings.isNullOrEmpty(catalogName)) {
            throw new IllegalArgumentException("catalog-name is required");
        }
        if (!catalogs.containsKey(catalogName)) {
            throw new CatalogNotFoundException(catalogName);
        }
        return catalogs.get(catalogName);
    }

    @Nonnull
    public Map<String, MetacatCatalogConfig> getCatalogs() {
        return ImmutableMap.copyOf(catalogs);
    }

    /**
     * Returns the connector factory for the given <code>catalogName</code>.
     *
     * @param catalogName catalog name
     * @return Returns the connector factory for the given <code>catalogName</code>
     */
    private ConnectorFactory getConnectorFactory(final String catalogName) {
        Preconditions.checkNotNull(catalogName, "catalogName is null");
        final ConnectorFactory result = connectorFactories.get(catalogName);
        if (result == null) {
            throw new CatalogNotFoundException(catalogName);
        }
        return result;
    }

    /**
     * Returns the connector plugin for the given <code>catalogName</code>.
     *
     * @param connectorType connector type
     * @return Returns the plugin for the given <code>catalogName</code>
     */
    private ConnectorPlugin getPlugin(final String connectorType) {
        Preconditions.checkNotNull(connectorType, "connectorType is null");
        final ConnectorPlugin result = plugins.get(connectorType);
        Preconditions.checkNotNull(result, "No connector plugin exists for type %s", connectorType);
        return result;
    }

    /**
     * Returns the connector database service for the given <code>catalogName</code>.
     *
     * @param catalogName catalog name
     * @return Returns the connector database service for the given <code>catalogName</code>
     */
    public ConnectorDatabaseService getDatabaseService(final String catalogName) {
        return getConnectorFactory(catalogName).getDatabaseService();
    }

    /**
     * Returns the connector table service for the given <code>catalogName</code>.
     *
     * @param catalogName catalog name
     * @return Returns the connector table service for the given <code>catalogName</code>
     */
    public ConnectorTableService getTableService(final String catalogName) {
        return getConnectorFactory(catalogName).getTableService();
    }

    /**
     * Returns the connector partition service for the given <code>catalogName</code>.
     *
     * @param catalogName catalog name
     * @return Returns the connector partition service for the given <code>catalogName</code>
     */
    public ConnectorPartitionService getPartitionService(final String catalogName) {
        return getConnectorFactory(catalogName).getPartitionService();
    }

    /**
     * Returns the connector type converter for the given <code>connectorType</code>.
     *
     * @param connectorType connector type
     * @return Returns the connector type converter for the given <code>connectorType</code>
     */
    public ConnectorTypeConverter getTypeConverter(final String connectorType) {
        return getPlugin(connectorType).getTypeConverter();
    }

    /**
     * Returns the connector dto converter for the given <code>connectorType</code>.
     *
     * @param connectorType connector type
     * @return Returns the connector dto converter for the given <code>connectorType</code>
     */
    public ConnectorInfoConverter getInfoConverter(final String connectorType) {
        return getPlugin(connectorType).getInfoConverter();
    }
}
