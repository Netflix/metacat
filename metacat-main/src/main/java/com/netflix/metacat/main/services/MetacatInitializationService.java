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
package com.netflix.metacat.main.services;

import com.google.common.base.Throwables;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.main.manager.CatalogManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.manager.PluginManager;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Metacat initialization service.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Slf4j
public class MetacatInitializationService implements HealthIndicator {
    protected static final String PLUGIN_KEY = "pluginsLoaded";
    protected static final String CATALOG_KEY = "catalogsLoaded";
    protected static final String THRIFT_KEY = "thriftStarted";

    @NonNull
    private final PluginManager pluginManager;
    @NonNull
    private final CatalogManager catalogManager;
    @NonNull
    private final ConnectorManager connectorManager;
    @NonNull
    private final ThreadServiceManager threadServiceManager;
    @NonNull
    private final MetacatThriftService metacatThriftService;
    // Initial values are false
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final AtomicBoolean thriftStarted = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param pluginManager        Plugin manager to use
     * @param catalogManager       Catalog manager to use
     * @param connectorManager     Connector manager to use
     * @param threadServiceManager Thread service manager to use
     * @param metacatThriftService Metacat thrift service implementation to use
     */
    public MetacatInitializationService(
        final PluginManager pluginManager,
        final CatalogManager catalogManager,
        final ConnectorManager connectorManager,
        final ThreadServiceManager threadServiceManager,
        final MetacatThriftService metacatThriftService
    ) {
        this.pluginManager = pluginManager;
        this.catalogManager = catalogManager;
        this.connectorManager = connectorManager;
        this.threadServiceManager = threadServiceManager;
        this.metacatThriftService = metacatThriftService;

        this.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Health health() {
        final boolean plugins = this.pluginsLoaded.get();
        final boolean catalogs = this.catalogsLoaded.get();
        final boolean thrift = this.thriftStarted.get();

        final Health.Builder builder = plugins && catalogs && thrift ? Health.up() : Health.outOfService();

        builder.withDetail(PLUGIN_KEY, plugins);
        builder.withDetail(CATALOG_KEY, catalogs);
        builder.withDetail(THRIFT_KEY, thrift);

        return builder.build();
    }

    /**
     * Metacat service shutdown.
     *
     * @param event Event when the context is shutting down
     */
    @EventListener
    public void stop(final ContextClosedEvent event) {
        log.info("Metacat application is stopped per {}. Stopping services.", event);
        try {
            this.pluginsLoaded.set(false);
            this.connectorManager.stop();
            this.catalogsLoaded.set(false);
            this.threadServiceManager.stop();
            this.metacatThriftService.stop();
            this.thriftStarted.set(false);
        } catch (final Exception e) {
            // Just log it since we're shutting down anyway shouldn't matter to propagate it
            log.error("Unable to properly shutdown services due to {}", e.getMessage(), e);
        }
        log.info("Finished stopping services.");
    }

    /**
     * Metacat service initialization.
     */
    public void start() {
        log.info("Metacat application starting. Starting internal services...");
        try {
            // TODO: Rather than doing this statically why don't we have things that need to be started implement
            //       some interface/order?
            this.pluginManager.loadPlugins();
            this.pluginsLoaded.set(true);
            this.catalogManager.loadCatalogs();
            this.catalogsLoaded.set(true);
            this.metacatThriftService.start();
            this.thriftStarted.set(true);
        } catch (final Exception e) {
            log.error("Unable to initialize services due to {}", e.getMessage(), e);
            Throwables.propagate(e);
        }
        log.info("Finished starting internal services.");
    }
}
