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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.netflix.metacat.main.spi.MetacatCatalogConfig.createFromMapAndRemoveProperties;

@Singleton
public class MetacatConnectorManager extends ConnectorManager {
    private static final Logger log = LoggerFactory.getLogger(MetacatConnectorManager.class);
    private final ConcurrentHashMap<String, MetacatCatalogConfig> catalogs = new ConcurrentHashMap<>();

    @Inject
    public MetacatConnectorManager(MetadataManager metadataManager,
        SplitManager splitManager,
        HandleResolver handleResolver,
        Map<String, ConnectorFactory> connectorFactories) {
        super(metadataManager, splitManager, handleResolver, connectorFactories);
    }

    @Override
    public synchronized void createConnection(
        String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties) {
        properties = Maps.newHashMap(properties);
        MetacatCatalogConfig config = createFromMapAndRemoveProperties(connectorFactory.getName(), properties);

        super.createConnection(catalogName, connectorFactory, properties);

        catalogs.put(catalogName, config);
    }

    @Nonnull
    public MetacatCatalogConfig getCatalogConfig(QualifiedName name) {
        return getCatalogConfig(name.getCatalogName());
    }

    @Nonnull
    public MetacatCatalogConfig getCatalogConfig(String catalogName) {
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
