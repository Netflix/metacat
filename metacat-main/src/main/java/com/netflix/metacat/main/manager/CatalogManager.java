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
package com.netflix.metacat.main.manager;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Catalog manager. This loads the catalogs defined as .properties files under the location defined by config property
 * <code>metacat.plugin.config.location</code>.
 * Usually there is a one-to-one mapping between a catalog and a data store. We could also have data stores
 * (mostly sharded) addressed by a single catalog name.
 * If a data store is sharded and needs to be represented by a single catalog name, then we will have multiple catalog
 * property files, each referencing to its physical data store and having the same <code>catalog.name</code>.
 * In this case, <code>catalog.name</code> and <code>metacat.schema.whitelist</code> will be used to point to the right
 * data store. A catalog with no <code>metacat.schema.whitelist</code> setting will be the default catalog representing
 * all databases for the <code>catalog.name</code>.
 */
@Slf4j
public class CatalogManager implements HealthIndicator {
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final Registry registry;
    private final Config config;

    /**
     * Constructor.
     *
     * @param connectorManager manager
     * @param config           config
     * @param registry         registry of spectator
     */
    public CatalogManager(
        final ConnectorManager connectorManager,
        final Config config,
        final Registry registry
    ) {
        this.connectorManager = connectorManager;
        this.config = config;
        this.catalogConfigurationDir = new File(config.getPluginConfigLocation());
        this.registry = registry;
    }

    /**
     * Returns true if all catalogs are loaded.
     *
     * @return true if all catalogs are loaded
     */
    public boolean areCatalogsLoaded() {
        return this.catalogsLoaded.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        // If catalogs are loaded we'll just report up
        final Health.Builder builder = this.catalogsLoaded.get() ? Health.up() : Health.outOfService();

        this.connectorManager.getCatalogs().forEach(catalog -> {
            final String key = catalog.getSchemaWhitelist().isEmpty() ? catalog.getCatalogName()
                : String.format("%s:%s", catalog.getCatalogName(), catalog.getSchemaWhitelist());
            builder.withDetail(key, catalog.getType());
        });

        return builder.build();
    }

    /**
     * Loads catalogs.
     *
     * @throws Exception error
     */
    public void loadCatalogs() throws Exception {
        if (!this.catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (final File file : this.listFiles(this.catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                this.loadCatalog(file);
            }
        }

        this.catalogsLoaded.set(true);
    }

    private void loadCatalog(final File file) throws Exception {
        log.info("-- Loading catalog {} --", file);
        final Map<String, String> properties = new HashMap<>(this.loadProperties(file));

        final String connectorType = properties.remove(MetacatCatalogConfig.Keys.CONNECTOR_NAME);
        Preconditions.checkState(
            connectorType != null,
            "Catalog configuration %s does not contain connector.name",
            file.getAbsoluteFile()
        );

        // Catalog shard name should be unique. Usually the catalog name is same as the catalog shard name.
        // If multiple catalog property files refer the same catalog name, then there will be multiple shard names
        // with the same catalog name.
        final String catalogShardName = Files.getNameWithoutExtension(file.getName());
        // If catalog name is not specified, then use the catalog shard name.
        final String catalogName = properties.getOrDefault(MetacatCatalogConfig.Keys.CATALOG_NAME, catalogShardName);
        final ConnectorContext connectorContext =
            new ConnectorContext(catalogName, catalogShardName, connectorType, config, registry, properties);
        this.connectorManager.createConnection(connectorContext);
        log.info("-- Added catalog {} shard {} using connector {} --", catalogName, catalogShardName, connectorType);
    }

    private List<File> listFiles(final File installedPluginsDir) {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            final File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private Map<String, String> loadProperties(final File file)
        throws Exception {
        Preconditions.checkNotNull(file, "file is null");

        final Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return Maps.fromProperties(properties);
    }
}
