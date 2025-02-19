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
package com.netflix.metacat.main.manager;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorCatalogService;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorFactoryDecorator;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorPlugin;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.connectors.exception.CatalogNotFoundException;
import com.netflix.metacat.common.server.connectors.model.CatalogInfo;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Strings;


import jakarta.annotation.PreDestroy;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Connector manager.
 */
@Slf4j
@RequiredArgsConstructor
public class ConnectorManager {
    private static final String EMPTY_STRING = "";
    // Map of connector plugins registered.
    private final ConcurrentMap<String, ConnectorPlugin> plugins = new ConcurrentHashMap<>();
    /**
     * Table of catalog name, database name to catalog data stores. Usually there is a one-to-one mapping between the
     * catalog name and the data store. In this case the table will have the databse name as null. If there are multiple
     * catalogs addressed by the same <code>catalog.name</code> pointing to shards of a data store, then there will be
     * multiple entries in this table. An entry will be identified using the catalog name and the database name.
     */
    private final Table<String, String, CatalogHolder> catalogs = HashBasedTable.create();
    private final Set<MetacatCatalogConfig> catalogConfigs = Sets.newHashSet();
    private final Set<ConnectorDatabaseService> databaseServices = Sets.newHashSet();
    private final Set<ConnectorTableService> tableServices = Sets.newHashSet();
    private final Set<ConnectorPartitionService> partitionServices = Sets.newHashSet();
    private final AtomicBoolean stopped = new AtomicBoolean();

    private final Config config;

    /**
     * Stop.
     */
    @PreDestroy
    public void stop() {
        if (stopped.getAndSet(true)) {
            return;
        }
        catalogs.values().forEach(catalogHolder -> {
            try {
                catalogHolder.getConnectorFactory().stop();
            } catch (Throwable t) {
                log.error("Error shutting down connector: {}", catalogHolder.getConnectorFactory().getCatalogName(), t);
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
     * @param connectorContext metacat connector properties
     */
    public synchronized void createConnection(final ConnectorContext connectorContext) {
        Preconditions.checkState(!stopped.get(), "ConnectorManager is stopped");
        final String connectorType = connectorContext.getConnectorType();
        final String catalogName = connectorContext.getCatalogName();
        final String catalogShardName = connectorContext.getCatalogShardName();
        final ConnectorPlugin connectorPlugin = plugins.get(connectorType);

        if (connectorPlugin != null) {
            final MetacatCatalogConfig catalogConfig =
                MetacatCatalogConfig.createFromMapAndRemoveProperties(connectorType, catalogName,
                    connectorContext.getConfiguration());
            final List<String> databaseNames = catalogConfig.getSchemaWhitelist();
            if (databaseNames.isEmpty()) {
                Preconditions.checkState(!catalogs.contains(catalogName, EMPTY_STRING),
                    "A catalog with name %s already exists", catalogName);
            } else {
                databaseNames.forEach(databaseName -> {
                    Preconditions.checkState(!catalogs.contains(catalogName, databaseName),
                        "A catalog with name %s for database %s already exists", catalogName, databaseName);
                });
            }

            catalogConfigs.add(catalogConfig);

            final ConnectorFactory connectorFactory = new ConnectorFactoryDecorator(connectorPlugin, connectorContext);

            try {
                databaseServices.add(connectorFactory.getDatabaseService());
            } catch (UnsupportedOperationException e) {
                log.debug("Catalog {}:{} doesn't support getDatabaseService. Ignoring.", catalogName, catalogShardName);
            }

            try {
                tableServices.add(connectorFactory.getTableService());
            } catch (UnsupportedOperationException e) {
                log.debug("Catalog {}:{} doesn't support getTableService. Ignoring.", catalogName, catalogShardName);
            }

            try {
                partitionServices.add(connectorFactory.getPartitionService());
            } catch (UnsupportedOperationException e) {
                log.debug("Catalog {}:{} doesn't support getPartitionService. Ignoring.",
                    catalogName, catalogShardName);
            }

            final CatalogHolder catalogHolder = new CatalogHolder(catalogConfig, connectorFactory);
            if (databaseNames.isEmpty()) {
                catalogs.put(catalogName, EMPTY_STRING, catalogHolder);
            } else {
                databaseNames.forEach(databaseName -> {
                    catalogs.put(catalogName, databaseName, catalogHolder);
                });
            }
        } else {
            log.warn("No plugin for connector with type {}", connectorType);
        }
    }

    /**
     * Returns a set of catalog holders.
     *
     * @param catalogName catalog name
     * @return catalog holders
     */
    @Nonnull
    private Set<CatalogHolder> getCatalogHolders(final String catalogName) {
        final Map<String, CatalogHolder> result =  getCatalogHoldersByDatabaseName(catalogName);
        if (result.isEmpty()) {
            throw new CatalogNotFoundException(catalogName);
        } else {
            return Sets.newHashSet(result.values());
        }
    }


    private Map<String, CatalogHolder> getCatalogHoldersByDatabaseName(final String catalogName) {
        Map<String, CatalogHolder> result =  catalogs.row(catalogName);
        if (result.isEmpty()) {
            final String proxyCatalogName = getConnectorNameFromCatalogName(catalogName);
            if (!Strings.isNullOrEmpty(proxyCatalogName)) {
                result = catalogs.row(proxyCatalogName);
            }
        }
        return result;
    }

    /**
     * This method should be called only for a proxy catalog. A proxy catalog is a connector catalog that acts as a
     * proxy to another service that contains the actual list of catalogs. The convention of the naming is such that
     * the connector name is prefixed to the catalog names. Ex: For a catalog configuration with name as 'cde', the
     * catalogs under it will be prefixed by 'cde_'.
     *
     * @param catalogName catalog name
     * @return connector name
     */
    private String getConnectorNameFromCatalogName(final String catalogName) {
        String result = null;
        final Iterator<String> splits = Splitter.on("_").limit(2).split(catalogName).iterator();
        if (splits.hasNext()) {
            result = splits.next();
        }
        return result;
    }

    /**
     * Returns the catalog holder.
     *
     * @param name name
     * @return catalog holder
     */
    @Nonnull
    private CatalogHolder getCatalogHolder(final QualifiedName name) {
        final String catalogName = name.getCatalogName();
        final String databaseName = name.isDatabaseDefinition() ? name.getDatabaseName() : EMPTY_STRING;
        final Map<String, CatalogHolder> catalogHolders = getCatalogHoldersByDatabaseName(catalogName);
        final CatalogHolder result =  catalogHolders.containsKey(databaseName)
            ? catalogHolders.get(databaseName) : catalogHolders.get(EMPTY_STRING);
        if (result == null) {
            throw new CatalogNotFoundException(catalogName);
        }
        return result;
    }

    /**
     * Returns the catalog config based on the qualified name that may or may not have the database name.
     * If database name is not present, then the default catalog is returned if there are multiple catalogs with
     * the same catalog name.
     *
     * @param name name
     * @return catalog config
     */
    @Nonnull
    public MetacatCatalogConfig getCatalogConfig(final QualifiedName name) {
        return getCatalogHolder(name).getCatalogConfig();
    }

    /**
     * Returns the catalog configs based on the catalog name. In the case where there are multiple catalogs with the
     * same catalog name, this method will return multiple catalog configs.
     *
     * @param name name
     * @return set of catalog configs
     */
    @Nonnull
    public Set<MetacatCatalogConfig> getCatalogConfigs(final String name) {
        return getCatalogHolders(name).stream().map(CatalogHolder::getCatalogConfig).collect(Collectors.toSet());
    }

    /**
     * Returns all catalog configs. In the case where a catalog is a proxy connector, the list of catalogs represented
     * by the connector will not be included.
     * @return  set of catalog configs
     */
    @Nonnull
    public Set<MetacatCatalogConfig> getCatalogConfigs() {
        return catalogConfigs;
    }

    /**
     * Returns all catalogs. The list will also include the list of catalogs represented by a proxy connector.
     * @return set of catalogs
     */
    @Nonnull
    public Set<CatalogInfo> getCatalogs() {
        return catalogs.column(EMPTY_STRING).values().stream().flatMap(c -> {
            final Stream.Builder<CatalogInfo> builder = Stream.builder();
            final MetacatCatalogConfig catalogConfig = c.getCatalogConfig();
            if (catalogConfig.isProxy()) {
                c.getConnectorFactory().getCatalogService()
                    .list(new ConnectorRequestContext(), QualifiedName.ofCatalog(catalogConfig.getCatalogName()),
                        null, null, null)
                    .forEach(builder);
            } else {
                builder.accept(catalogConfig.toCatalogInfo());
            }
            return builder.build();
        }).collect(Collectors.toSet());
    }

    /**
     * Returns the connector factory for the given <code>name</code>.
     *
     * @param name qualified name
     * @return Returns the connector factory for the given <code>name</code>
     */
    private ConnectorFactory getConnectorFactory(final QualifiedName name) {
        Preconditions.checkNotNull(name, "Name is null");
        return getCatalogHolder(name).getConnectorFactory();
    }

    /**
     * Returns the connector plugin for the given <code>catalogName</code>.
     *
     * @param connectorType connector type
     * @return Returns the plugin for the given <code>catalogName</code>
     */
    public ConnectorPlugin getPlugin(final String connectorType) {
        Preconditions.checkNotNull(connectorType, "connectorType is null");
        final ConnectorPlugin result = plugins.get(connectorType);
        Preconditions.checkNotNull(result, "No connector plugin exists for type %s", connectorType);
        return result;
    }

    /**
     * Returns all the connector database services.
     *
     * @return Returns all the connector database services registered in the system.
     */
    public Set<ConnectorDatabaseService> getDatabaseServices() {
        return databaseServices;
    }

    /**
     * Returns all the connector table services.
     *
     * @return Returns all the connector table services registered in the system.
     */
    public Set<ConnectorTableService> getTableServices() {
        return tableServices;
    }

    /**
     * Returns all the connector partition services.
     *
     * @return Returns all the connector partition services registered in the system.
     */
    public Set<ConnectorPartitionService> getPartitionServices() {
        return partitionServices;
    }

    /**
     * Returns the connector catalog service for the given <code>name</code>.
     *
     * @param name qualified name
     * @return Returns the connector catalog service for the given <code>name</code>
     */
    public ConnectorCatalogService getCatalogService(final QualifiedName name) {
        return getConnectorFactory(name).getCatalogService();
    }

    /**
     * Returns the connector database service for the given <code>name</code>.
     *
     * @param name qualified name
     * @return Returns the connector database service for the given <code>name</code>
     */
    public ConnectorDatabaseService getDatabaseService(final QualifiedName name) {
        return getConnectorFactory(name).getDatabaseService();
    }

    /**
     * Returns the connector table service for the given <code>name</code>.
     *
     * @param name qualified name
     * @return Returns the connector table service for the given <code>name</code>
     */
    public ConnectorTableService getTableService(final QualifiedName name) {
        return getConnectorFactory(name).getTableService();
    }

    /**
     * Returns the connector partition service for the given <code>name</code>.
     *
     * @param name qualified name
     * @return Returns the connector partition service for the given <code>name</code>
     */
    public ConnectorPartitionService getPartitionService(final QualifiedName name) {
        return getConnectorFactory(name).getPartitionService();
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

    /**
     * A Holder class holding the catalog's config and connector factory.
     */
    @Data
    @AllArgsConstructor
    private static class CatalogHolder {
        private final MetacatCatalogConfig catalogConfig;
        private final ConnectorFactory connectorFactory;
    }
}
