package com.netflix.metacat.common.server.connectors;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Plugin interface implemented by Connectors.
 *
 * @author amajumdar
 * @since 1.0.0
 */
public interface ConnectorPlugin {

    /**
     * Returns the type of the plugin.
     *
     * @return Returns the type of the plugin.
     */
    String getType();

    /**
     * Returns the service implementation for the type.
     * @param connectorName connector name. This is also the catalog name.
     * @param configuration configuration properties
     * @return connector factory
     */
    ConnectorFactory create(@Nonnull String connectorName, @Nonnull Map<String, String> configuration);


    /**
     * Returns the partition service implementation of the connector.
     *
     * @return Returns the partition service implementation of the connector.
     */
    ConnectorTypeConverter getTypeConverter();

    /**
     * Returns the dto converter implementation of the connector.
     *
     * @return Returns the dto converter implementation of the connector.
     */
    default ConnectorInfoConverter getInfoConverter() {
        return new ConnectorInfoConverter() { };
    }
}
