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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import java.io.IOException;
import java.util.List;

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

    private static final Logger log = LoggerFactory.getLogger(MetacatContextListener.class);

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        log.info("Start contextDestroyed");
        super.contextDestroyed(sce);
        // Stop logging
        LoggingConfiguration.getInstance().stop();
        MetacatInitializationService service = getInjector().getInstance(MetacatInitializationService.class);
        try {
            service.stop();
        } catch (Throwable t) {
            log.error("Error stopping Metacat", t);
            throw Throwables.propagate(t);
        }
        log.info("Finish contextDestroyed");
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        log.info("Start contextInitialized");
        super.contextInitialized(sce);

        Config config = getInjector().getInstance(Config.class);
        MetacatInitializationService service = getInjector().getInstance(MetacatInitializationService.class);
        try {
            service.start();
        } catch (Throwable t) {
            log.error("Error initializing Metacat", t);
            throw Throwables.propagate(t);
        }
        // Configure and Initialize swagger using
        BeanConfig beanConfig = new BeanConfig();
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
