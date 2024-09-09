package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.ConnectorPlugin;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import lombok.NonNull;

import javax.annotation.Nonnull;
/**
 * Polaris Connector Plugin.
 */
public class PolarisConnectorPlugin implements ConnectorPlugin {

    private static final String CONNECTOR_TYPE = "polaris";
    private static final HiveTypeConverter TYPE_CONVERTER =
        new HiveTypeConverter(ConnectorContext.builder().build().getConfig());
    private static final HiveConnectorInfoConverter INFO_CONVERTER
        = new HiveConnectorInfoConverter(TYPE_CONVERTER);

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
