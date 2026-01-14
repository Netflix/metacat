package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.ConnectorPlugin;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.converter.IcebergTypeConverter;
import lombok.NonNull;

import javax.annotation.Nonnull;
/**
 * Polaris Connector Plugin.
 */
public class PolarisConnectorPlugin implements ConnectorPlugin {

    private static final String CONNECTOR_TYPE = "polaris";
    private static final IcebergTypeConverter TYPE_CONVERTER = new IcebergTypeConverter();
    private static final PolarisConnectorInfoConverter INFO_CONVERTER = new PolarisConnectorInfoConverter();

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
        return new PolarisConnectorFactory(INFO_CONVERTER, connectorContext);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTypeConverter getTypeConverter() {
        return TYPE_CONVERTER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorInfoConverter getInfoConverter() {
        return INFO_CONVERTER;
    }
}
