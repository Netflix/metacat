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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class MetacatInitializationService {
    private static final Logger log = LoggerFactory.getLogger(MetacatInitializationService.class);
    private final Config config;
    private final ExecutorService eventExecutor;
    private final Injector injector;

    @Inject
    public MetacatInitializationService(Injector injector, Config config) {
        this.config = config;
        this.eventExecutor = Executors.newFixedThreadPool(config.getEventBusExecutorThreadCount());
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

        // Start the thrift services
        MetacatThriftService thriftService = injector.getInstance(MetacatThriftService.class);
        thriftService.start();

        MetacatEventBus eventBus = new MetacatEventBus(eventExecutor);
        // Initialize elastic search client
        Client client = injector.getInstance(Client.class);
        if( client != null){
            MetacatEventHandlers handlers = injector.getInstance(MetacatEventHandlers.class);
            eventBus.register(handlers);
        }
    }

    public void stop() throws Exception {
        injector.getInstance(UserMetadataService.class).stop();

        // Start the thrift services
        MetacatThriftService thriftService = injector.getInstance(MetacatThriftService.class);
        thriftService.stop();

        // Shutdown the executor used for event bus
        if(eventExecutor != null){
            // Make the executor accept no new threads and finish all existing
            // threads in the queue
            eventExecutor.shutdown();
            try {
                eventExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                log.error("Error while shutting down executor service : ", e);
            }
        }
    }
}
