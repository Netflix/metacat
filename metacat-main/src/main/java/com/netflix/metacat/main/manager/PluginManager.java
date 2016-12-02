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

import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.netflix.metacat.main.presto.connector.ConnectorManager;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plugin Manager.
 */
@Singleton
@Slf4j
public class PluginManager {
    private final ConnectorManager connectorManager;
    private final Injector injector;
    private final Map<String, String> optionalConfig;
    private final AtomicBoolean pluginsLoaded = new AtomicBoolean();
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();
    private final TypeRegistry typeRegistry;

    /**
     * Constructor.
     * @param injector injector
     * @param connectorManager manager
     * @param typeRegistry registry
     */
    @Inject
    public PluginManager(final Injector injector,
        final ConnectorManager connectorManager,
        final TypeRegistry typeRegistry) {
        Preconditions.checkNotNull(injector, "injector is null");

        this.injector = injector;

        optionalConfig = Maps.newConcurrentMap();

        this.connectorManager = Preconditions.checkNotNull(connectorManager, "connectorManager is null");
        this.typeRegistry = Preconditions.checkNotNull(typeRegistry, "typeRegistry is null");
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
     * @param plugin service plugin
     */
    public void installPlugin(final Plugin plugin) {
        injector.injectMembers(plugin);

        plugin.setOptionalConfig(optionalConfig);

        for (Type type : plugin.getServices(Type.class)) {
            log.info("Registering type {}", type.getTypeSignature());
            typeRegistry.addType(type);
        }

        for (ConnectorFactory connectorFactory : plugin.getServices(ConnectorFactory.class)) {
            log.info("Registering connector {}", connectorFactory.getName());
            connectorManager.addConnectorFactory(connectorFactory);
        }
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

        final ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, this.getClass().getClassLoader());
        final List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);

        if (plugins.isEmpty()) {
            log.warn("No service providers of type {}", Plugin.class.getName());
        }

        for (Plugin plugin : plugins) {
            log.info("Installing {}", plugin.getClass().getName());
            installPlugin(plugin);
            log.info("-- Finished loading plugin {} --", plugin.getClass().getName());
        }

        pluginsLoaded.set(true);
    }
}
