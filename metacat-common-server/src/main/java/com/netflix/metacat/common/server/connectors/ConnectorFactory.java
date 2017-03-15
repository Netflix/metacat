package com.netflix.metacat.common.server.connectors;

/**
 * Factory that returns the connector implementations of the service and converter interfaces.
 * @author amajumdar
 * @since 1.0.0
 */
public interface ConnectorFactory {
    /**
     * Standard error message for all default implementations.
     */
    String UNSUPPORTED_MESSAGE = "Not supported by this connector";

    /**
     * Returns the database service implementation of the connector.
     * @return Returns the database service implementation of the connector.
     */
    default ConnectorDatabaseService getDatabaseService() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table service implementation of the connector.
     * @return Returns the table service implementation of the connector.
     */
    default ConnectorTableService getTableService() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the partition service implementation of the connector.
     * @return Returns the partition service implementation of the connector.
     */
    default ConnectorPartitionService getPartitionService() {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the name of the connector.
     * @return Returns the name of the connector.
     */
    String getName();

    /**
     * Shuts down the factory.
     */
    void stop();
}
