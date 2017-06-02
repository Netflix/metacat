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

import com.google.inject.Injector;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.manager.PluginManager;
import com.netflix.metacat.main.manager.CatalogManager;
import com.netflix.metacat.main.services.notifications.NotificationService;
import com.netflix.metacat.main.services.search.MetacatEventHandlers;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.Client;

import javax.inject.Inject;
import java.util.Set;

/**
 * Metacat initialization service.
 */
@Slf4j
public class MetacatInitializationService {
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
        this.injector = injector;

        // Register all the services to listen for events
        notificationServices.forEach((object) -> eventBus.register(object, "sns"));
    }

    /**
     * Metacat service initialization.
     *
     * @throws Exception error
     */
    public void start() throws Exception {
        injector.getInstance(PluginManager.class).loadPlugins();
        injector.getInstance(CatalogManager.class).loadCatalogs();
        // Initialize user metadata service
        injector.getInstance(UserMetadataService.class).start();
        // Initialize the default thread pool for use in the service
        injector.getInstance(ThreadServiceManager.class).start();
        // Start the thrift services
        injector.getInstance(MetacatThriftService.class).start();
        // Initialize elastic search client
        final Client client = injector.getInstance(Client.class);
        if (client != null) {
            final MetacatEventBus eventBus = injector.getInstance(MetacatEventBus.class);
            // Only register the elastic search event handlers if the client is registered
            final MetacatEventHandlers handlers = injector.getInstance(MetacatEventHandlers.class);
            eventBus.register(handlers, "es");
        }
    }

    /**
     * Metcat service shutdown.
     *
     * @throws Exception error
     */
    public void stop() throws Exception {
        injector.getInstance(ConnectorManager.class).stop();
        injector.getInstance(UserMetadataService.class).stop();
        injector.getInstance(MetacatEventBus.class).shutdown();
        injector.getInstance(ThreadServiceManager.class).stop();
        // Stop the thrift services
        injector.getInstance(MetacatThriftService.class).stop();
    }
}
