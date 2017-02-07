package com.netflix.metacat.common.server.connectors;

/**
 * Plugin interface implemented by Connectors.
 *
 * @author amajumdar
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
     *
     * @return connector factory
     */
    ConnectorFactory create();


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
    ConnectorInfoConverter getDtoConverter();
}
