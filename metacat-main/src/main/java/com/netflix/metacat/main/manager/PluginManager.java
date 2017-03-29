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

package com.netflix.metacat.main.manager;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.netflix.metacat.common.server.connectors.ConnectorPlugin;
import com.netflix.metacat.common.server.converter.TypeConverterFactory;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plugin Manager.
 */
@Slf4j
public class PluginManager {
    private final ConnectorManager connectorManager;
    private final TypeConverterFactory typeConverterProvider;
    private final Injector injector;
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();

    /**
     * Constructor.
     * @param injector injector
     * @param connectorManager manager
     * @param typeConverterProvider provider for type converters
     */
    @Inject
    public PluginManager(final Injector injector, final ConnectorManager connectorManager,
        final TypeConverterFactory typeConverterProvider) {
        Preconditions.checkNotNull(injector, "injector is null");
        this.injector = injector;
        this.connectorManager = Preconditions.checkNotNull(connectorManager, "connectorManager is null");
        this.typeConverterProvider = Preconditions.checkNotNull(typeConverterProvider, "typeConverterProvider is null");
    }

    /**
     * Returns true if plugins are loaded.
     * @return true if plugins are loaded.
     */
    public boolean arePluginsLoaded() {
        return pluginsLoaded.get();
    }

    /**
     * Installs the plugins.
     * @param connectorPlugin service plugin
     */
    public void installPlugin(final ConnectorPlugin connectorPlugin) {
        injector.injectMembers(connectorPlugin);
        connectorManager.addPlugin(connectorPlugin);
        typeConverterProvider.register(connectorPlugin.getType(), connectorPlugin.getTypeConverter());
    }

    /**
     * Loads the plugins.
     * @throws Exception error
     */
    public void loadPlugins()
        throws Exception {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        final ServiceLoader<ConnectorPlugin> serviceLoader =
            ServiceLoader.load(ConnectorPlugin.class, this.getClass().getClassLoader());
        final List<ConnectorPlugin> connectorPlugins = ImmutableList.copyOf(serviceLoader);

        if (connectorPlugins.isEmpty()) {
            log.warn("No service providers of type {}", ConnectorPlugin.class.getName());
        }

        for (ConnectorPlugin connectorPlugin : connectorPlugins) {
            log.info("Installing {}", connectorPlugin.getClass().getName());
            installPlugin(connectorPlugin);
            log.info("-- Finished loading plugin {} --", connectorPlugin.getClass().getName());
        }

        pluginsLoaded.set(true);
    }
}
