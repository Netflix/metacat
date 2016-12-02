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

package com.netflix.metacat.main.connector;

import com.facebook.presto.exception.CatalogNotFoundException;
import com.facebook.presto.spi.ConnectorFactory;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.main.presto.connector.ConnectorManager;
import com.netflix.metacat.main.presto.metadata.HandleResolver;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.presto.split.SplitManager;
import com.netflix.metacat.main.spi.MetacatCatalogConfig;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metacat connector manager.
 */
@Singleton
@Slf4j
public class MetacatConnectorManager extends ConnectorManager {
    private final ConcurrentHashMap<String, MetacatCatalogConfig> catalogs = new ConcurrentHashMap<>();

    /**
     * Constructor.
     * @param metadataManager manager
     * @param splitManager split manager
     * @param handleResolver resolver
     * @param connectorFactories connctor factories
     */
    @Inject
    public MetacatConnectorManager(final MetadataManager metadataManager,
        final SplitManager splitManager,
        final HandleResolver handleResolver,
        final Map<String, ConnectorFactory> connectorFactories) {
        super(metadataManager, splitManager, handleResolver, connectorFactories);
    }

    @Override
    public synchronized void createConnection(
        final String catalogName, final ConnectorFactory connectorFactory, final Map<String, String> mProperties) {
        final Map<String, String> properties = Maps.newHashMap(mProperties);
        final MetacatCatalogConfig config =
            MetacatCatalogConfig.createFromMapAndRemoveProperties(connectorFactory.getName(), properties);

        super.createConnection(catalogName, connectorFactory, properties);

        catalogs.put(catalogName, config);
    }

    /**
     * Returns the catalog config.
     * @param name name
     * @return catalog config
     */
    @Nonnull
    public MetacatCatalogConfig getCatalogConfig(final QualifiedName name) {
        return getCatalogConfig(name.getCatalogName());
    }

    /**
     * Returns the catalog config.
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
}
