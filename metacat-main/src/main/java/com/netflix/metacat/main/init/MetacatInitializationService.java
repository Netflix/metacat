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
import com.netflix.metacat.main.services.search.MetacatEventHandlers;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationProvider;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class MetacatInitializationService {
    private static final Logger log = LoggerFactory.getLogger(MetacatInitializationService.class);
    private final Config config;
    private final Injector injector;

    @Inject
    public MetacatInitializationService(Injector injector, Config config) {
        this.config = config;
        this.injector = injector;
    }

    public ConfigurationFactory getConfigurationFactory() {
        String pluginConfigDir = config.getPluginConfigLocation();
        checkArgument(!Strings.isNullOrEmpty(pluginConfigDir),
                "Missing required property metacat.plugin.config.location");
        log.info("Loading catalogs from directory '{}'", pluginConfigDir);

        Map<String, String> properties = ImmutableMap.of("plugin.config-dir", pluginConfigDir);
        return new ConfigurationFactory(properties);
    }

    public void start() throws Exception {
        ConfigurationFactory configurationFactory = getConfigurationFactory();
        ProviderInstanceBinding<?> providerInstanceBinding = (ProviderInstanceBinding<?>) injector
                .getBinding(CatalogManagerConfig.class);
        Provider<?> provider = providerInstanceBinding.getProviderInstance();
        ((ConfigurationProvider) provider).setConfigurationFactory(configurationFactory);
        injector.getInstance(PluginManager.class).loadPlugins();
        injector.getInstance(CatalogManager.class).loadCatalogs();
        // Initialize user metadata service
        injector.getInstance(UserMetadataService.class).start();
        // Initialize the default thread pool for use in the service
        injector.getInstance(ThreadServiceManager.class).start();
        // Start the thrift services
        MetacatThriftService thriftService = injector.getInstance(MetacatThriftService.class);
        thriftService.start();

        // Initialize elastic search client
        Client client = injector.getInstance(Client.class);
        if (client != null) {
            MetacatEventBus eventBus = injector.getInstance(MetacatEventBus.class);
            // Only register the elastic search event handlers if the client is registered
            MetacatEventHandlers handlers = injector.getInstance(MetacatEventHandlers.class);
            eventBus.register(handlers);
        }
    }

    public void stop() throws Exception {
        injector.getInstance(UserMetadataService.class).stop();
        injector.getInstance(ThreadServiceManager.class).stop();
        // Start the thrift services
        MetacatThriftService thriftService = injector.getInstance(MetacatThriftService.class);
        thriftService.stop();
    }
}
