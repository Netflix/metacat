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

package com.netflix.metacat.server.init;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import com.netflix.blitz4j.LoggingConfiguration;
import com.netflix.config.ConfigurationManager;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.main.init.MetacatInitializationService;
import com.netflix.metacat.main.init.MetacatServletModule;
import com.netflix.metacat.usermetadata.mysql.MysqlUserMetadataModule;
import com.squarespace.jersey2.guice.JerseyGuiceServletContextListener;
import com.wordnik.swagger.jaxrs.config.BeanConfig;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletContextEvent;
import java.io.IOException;
import java.util.List;

/**
 * Servlet listener.
 */
@Slf4j
public class MetacatContextListener extends JerseyGuiceServletContextListener {
    static {
        // Initialize configuration
        System.setProperty("archaius.deployment.applicationId", "metacat");
        try {
            ConfigurationManager.loadCascadedPropertiesFromResources("metacat");
        } catch (IOException ignored) {
            //Do not stop the server initialization
        }
        //Initialize logging
        LoggingConfiguration.getInstance().configure();
    }

    @Override
    public void contextDestroyed(final ServletContextEvent sce) {
        log.info("Start contextDestroyed");
        super.contextDestroyed(sce);
        // Stop logging
        LoggingConfiguration.getInstance().stop();
        final MetacatInitializationService service = getInjector().getInstance(MetacatInitializationService.class);
        try {
            service.stop();
        } catch (Throwable t) {
            log.error("Error stopping Metacat", t);
            throw Throwables.propagate(t);
        }
        log.info("Finish contextDestroyed");
    }

    @Override
    public void contextInitialized(final ServletContextEvent sce) {
        log.info("Start contextInitialized");
        super.contextInitialized(sce);

        final Config config = getInjector().getInstance(Config.class);
        final MetacatInitializationService service = getInjector().getInstance(MetacatInitializationService.class);
        try {
            service.start();
        } catch (Throwable t) {
            log.error("Error initializing Metacat", t);
            throw Throwables.propagate(t);
        }
        // Configure and Initialize swagger using
        final BeanConfig beanConfig = new BeanConfig();
        beanConfig.setVersion(config.getMetacatVersion());
        beanConfig.setBasePath("/");
        beanConfig.setResourcePackage("com.netflix.metacat");
        beanConfig.setScan(true);

        log.info("Finish contextInitialized");
    }

    @Override
    protected List<? extends Module> modules() {
        return ImmutableList.of(
            new MetacatServletModule(),
            new MysqlUserMetadataModule()
        );
    }
}
