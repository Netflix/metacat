package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorPlugin;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import lombok.NonNull;

import javax.annotation.Nonnull;

/**
 * Polaris Connector Plugin.
 */
public class PolarisConnectorPlugin implements ConnectorPlugin {

    private static final String CONNECTOR_TYPE = "polaris";

    /**
     * {@inheritDoc}
     */
    @Override
    public String getType() {
        return CONNECTOR_TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorFactory create(@Nonnull @NonNull final ConnectorContext connectorContext) {
        return new PolarisConnectorFactory(connectorContext.getCatalogName(),
            connectorContext.getCatalogShardName(), connectorContext.getConfiguration());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTypeConverter getTypeConverter() {
        // TODO: add type converter
        return null;
    }
}
