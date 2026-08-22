package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.SpringConnectorFactory;
import com.netflix.metacat.connector.polaris.configs.PolarisConnectorConfig;
import com.netflix.metacat.connector.polaris.configs.PolarisDataSourceRegistry;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceReaderConfig;
import com.netflix.metacat.connector.polaris.configs.PolarisStoreConfig;
import org.springframework.core.env.MapPropertySource;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Map;

/**
 * Connector Factory for Polaris.
 */
class PolarisConnectorFactory extends SpringConnectorFactory {

    /**
     * Constructor.
     *
     * @param infoConverter    info converter
     * @param connectorContext connector config
     */
    PolarisConnectorFactory(
        final ConnectorInfoConverter infoConverter,
        final ConnectorContext connectorContext
    ) {
        super(infoConverter, connectorContext);
        super.addEnvProperties(new MapPropertySource(
            "polaris_connector", Collections.unmodifiableMap(connectorContext.getConfiguration())));
        super.registerClazz(PolarisConnectorConfig.class,
                PolarisPersistenceConfig.class,
            PolarisPersistenceReaderConfig.class,
            PolarisStoreConfig.class);

        final Map<String, String> config = connectorContext.getConfiguration();
        final String primaryUrl = config.get("spring.datasource.url");
        if (primaryUrl != null) {
            final DataSource shared = PolarisDataSourceRegistry.getOrCreatePrimary(primaryUrl, config);
            ctx.getBeanFactory().registerSingleton("dataSource", shared);
        }
        final String readerUrl = config.get("spring.datasource.reader.url");
        if (readerUrl != null) {
            final DataSource shared = PolarisDataSourceRegistry.getOrCreateReader(readerUrl, config);
            ctx.getBeanFactory().registerSingleton("readerDataSource", shared);
        }

        super.refresh();
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        return ctx.getBean(PolarisConnectorPartitionService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorDatabaseService getDatabaseService() {
        return this.ctx.getBean(PolarisConnectorDatabaseService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTableService getTableService() {
        return this.ctx.getBean(PolarisConnectorTableService.class);
    }
}
