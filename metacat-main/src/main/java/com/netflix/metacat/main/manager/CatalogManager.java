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
 * Catalog manager.
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

        for (final Map.Entry<String, MetacatCatalogConfig> entry : this.connectorManager.getCatalogs().entrySet()) {
            builder.withDetail(entry.getKey(), entry.getValue().getType());
        }

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

        final String connectorType = properties.remove("connector.name");
        Preconditions.checkState(
            connectorType != null,
            "Catalog configuration %s does not contain connector.name",
            file.getAbsoluteFile()
        );

        final String catalogName = Files.getNameWithoutExtension(file.getName());
        final ConnectorContext connectorContext = new ConnectorContext(catalogName, config, registry, properties);
        this.connectorManager.createConnection(catalogName, connectorType, connectorContext);
        log.info("-- Added catalog {} using connector {} --", catalogName, connectorType);
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
