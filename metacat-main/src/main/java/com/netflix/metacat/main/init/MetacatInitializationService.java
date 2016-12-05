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

package com.netflix.metacat.main.init;

import com.facebook.presto.metadata.CatalogManagerConfig;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.spi.ProviderInstanceBinding;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.ThreadServiceManager;
import com.netflix.metacat.main.manager.PluginManager;
import com.netflix.metacat.main.presto.metadata.CatalogManager;
import com.netflix.metacat.main.services.notifications.NotificationService;
import com.netflix.metacat.main.services.search.MetacatEventHandlers;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationProvider;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.Client;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;

/**
 * Metacat initialization service.
 */
@Slf4j
public class MetacatInitializationService {
    private final Config config;
    private final Injector injector;

    /**
     * Constructor.
     *
     * @param injector             injector
     * @param config               config
     * @param eventBus             The event bus to use for internal events
     * @param notificationServices The notification service implementations to register for receiving events
     */
    @Inject
    public MetacatInitializationService(
        final Injector injector,
        final Config config,
        final MetacatEventBus eventBus,
        final Set<NotificationService> notificationServices
    ) {
        this.config = config;
        this.injector = injector;

        // Register all the services to listen for events
        notificationServices.forEach(eventBus::register);
    }

    /**
     * Returns the config factory.
     *
     * @return config factory
     */
    public ConfigurationFactory getConfigurationFactory() {
        final String pluginConfigDir = config.getPluginConfigLocation();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(pluginConfigDir),
            "Missing required property metacat.plugin.config.location");
        log.info("Loading catalogs from directory '{}'", pluginConfigDir);

        final Map<String, String> properties = ImmutableMap.of("plugin.config-dir", pluginConfigDir);
        return new ConfigurationFactory(properties);
    }

    /**
     * Metacat service initialization.
     *
     * @throws Exception error
     */
    public void start() throws Exception {
        final ConfigurationFactory configurationFactory = getConfigurationFactory();
        final ProviderInstanceBinding<?> providerInstanceBinding = (ProviderInstanceBinding<?>) injector
            .getBinding(CatalogManagerConfig.class);
        final Provider<?> provider = providerInstanceBinding.getProviderInstance();
        ((ConfigurationProvider) provider).setConfigurationFactory(configurationFactory);
        injector.getInstance(PluginManager.class).loadPlugins();
        injector.getInstance(CatalogManager.class).loadCatalogs();
        // Initialize user metadata service
        injector.getInstance(UserMetadataService.class).start();
        // Initialize the default thread pool for use in the service
        injector.getInstance(ThreadServiceManager.class).start();
        // Start the thrift services
        final MetacatThriftService thriftService = injector.getInstance(MetacatThriftService.class);
        thriftService.start();

        // Initialize elastic search client
        final Client client = injector.getInstance(Client.class);
        if (client != null) {
            final MetacatEventBus eventBus = injector.getInstance(MetacatEventBus.class);
            // Only register the elastic search event handlers if the client is registered
            final MetacatEventHandlers handlers = injector.getInstance(MetacatEventHandlers.class);
            eventBus.register(handlers);
        }
    }

    /**
     * Metcat service shutdown.
     *
     * @throws Exception error
     */
    public void stop() throws Exception {
        injector.getInstance(UserMetadataService.class).stop();
        injector.getInstance(ThreadServiceManager.class).stop();
        // Start the thrift services
        final MetacatThriftService thriftService = injector.getInstance(MetacatThriftService.class);
        thriftService.stop();
    }
}
