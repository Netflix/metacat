package com.netflix.metacat.main.services.init;

import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.main.manager.CatalogManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.manager.PluginManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

/**
 * Inits the core catalog related dependencies.
 */
@Slf4j
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class MetacatCoreInitService {
    @NonNull
    private final PluginManager pluginManager;
    @NonNull
    private final CatalogManager catalogManager;
    @NonNull
    private final ConnectorManager connectorManager;
    @NonNull
    private final ThreadServiceManager threadServiceManager;
    @NonNull
    private final ApplicationContext applicationContext;

    /**
     * Metacat service shutdown.
     */
    public void stop() {
        log.info("Metacat application is stopped. Stopping services.");
        this.connectorManager.stop();
        this.threadServiceManager.stop();
    }

    /**
     * Metacat service initialization.
     *
     * @throws Exception if an error occurs during initialization.
     */
    public void start() throws Exception {
        log.info("Metacat application starting. Starting internal services...");
        this.pluginManager.loadPlugins();
        this.catalogManager.loadCatalogs(applicationContext);
    }
}
