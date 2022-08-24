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
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;
import com.netflix.metacat.common.server.util.MetacatUtils;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
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
public class DefaultCatalogManager implements CatalogManager {
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
    public DefaultCatalogManager(
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
    @Override
    public boolean areCatalogsLoaded() {
        return this.catalogsLoaded.get();
    }

    /**
     * Loads catalogs.
     *
     * @param applicationContext spring application context
     * @throws Exception error
     */
    @Override
    public void loadCatalogs(final ApplicationContext applicationContext) throws Exception {
        if (!this.catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (final File file : MetacatUtils.listFiles(this.catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                this.loadCatalog(file, applicationContext);
            }
        }

        this.catalogsLoaded.set(true);
    }

    protected void loadCatalog(final File file, final ApplicationContext applicationContext) throws Exception {
        log.info("-- Loading catalog {} --", file);
        final Map<String, String> properties = new HashMap<>(MetacatUtils.loadProperties(file));

        final String connectorType = properties.remove(MetacatCatalogConfig.Keys.CONNECTOR_NAME);
        Preconditions.checkState(
                connectorType != null,
                "Catalog configuration %s does not contain connector.name",
                file.getAbsoluteFile()
        );

        // Pass in the server application context to the connector context.
        final ConnectorContext connectorContext = MetacatUtils.buildConnectorContext(file,
                connectorType, config, registry, applicationContext, properties);
        this.connectorManager.createConnection(connectorContext);
        log.info("-- Added catalog {} shard {} using connector {} --",
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName(), connectorType);
    }
}
