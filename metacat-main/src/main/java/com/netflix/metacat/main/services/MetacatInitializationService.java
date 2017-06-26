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

package com.netflix.metacat.main.services;

import com.google.common.base.Throwables;
import com.netflix.metacat.thrift.MetacatThriftService;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.main.manager.CatalogManager;
import com.netflix.metacat.common.server.connectors.ConnectorManager;
import com.netflix.metacat.main.manager.PluginManager;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

/**
 * Metacat initialization service.
 */
@Slf4j
@AllArgsConstructor
public class MetacatInitializationService {
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

    /**
     * Metacat service initialization.
     *
     * @param event The context refreshed event that is fired whenever context initialized or refreshed
     */
    @EventListener
    public void start(final ContextRefreshedEvent event) {
        log.info("Metacat application started per {}. Starting services.", event);
        try {
            this.pluginManager.loadPlugins();
            this.catalogManager.loadCatalogs();
            this.metacatThriftService.start();
        } catch (final Exception e) {
            log.error("Unable to init services due to {}", e.getMessage(), e);
            Throwables.propagate(e);
        }
        log.info("Finished starting services.");
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
            this.connectorManager.stop();
            this.threadServiceManager.stop();
            this.metacatThriftService.stop();
        } catch (final Exception e) {
            // Just log it since we're shutting down anyway shouldn't matter to propagate it
            log.error("Unable to properly shutdown services due to {}", e.getMessage(), e);
        }
        log.info("Finished stopping services.");
    }
}
