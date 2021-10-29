package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.SpringConnectorFactory;
import com.netflix.metacat.connector.polaris.configs.PolarisConnectorConfig;
import com.netflix.metacat.connector.polaris.configs.PolarisPersistenceConfig;

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
        super.registerClazz(PolarisConnectorConfig.class,
            PolarisPersistenceConfig.class);
        super.refresh();
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
