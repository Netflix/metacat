package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import lombok.NonNull;

import javax.annotation.Nonnull;

/**
 * Class to convert Iceberg client exceptions to connector exceptions.
 */
public class PolarisExceptionMapper {
    /**
     * Convert the given Iceberg exception to a ConnectorException.
     *
     * @param e   The Iceberg client exception
     * @param name The fully qualified name of the resource attempted to be accessed or modified at time of error
     * @return A connector exception wrapping the DriverException
     */
    public ConnectorException toConnectorException(
        @Nonnull @NonNull final Exception e,
        @Nonnull @NonNull final QualifiedName name
    ) {
        // TODO: handling for exception more types
        return new ConnectorException(e.getMessage());
    }
}
