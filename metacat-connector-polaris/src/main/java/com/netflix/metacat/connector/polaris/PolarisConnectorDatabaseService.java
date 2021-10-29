package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.polaris.data.PolarisConnector;

/**
 * database service for polaris connector.
 */
public class PolarisConnectorDatabaseService implements ConnectorDatabaseService {
    private final PolarisConnector polarisConnector;

    /**
     * Constructor.
     *
     * @param polarisConnector polaris connector
     */
    public PolarisConnectorDatabaseService(
        final PolarisConnector polarisConnector
    ) {
        this.polarisConnector = polarisConnector;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void create(final ConnectorRequestContext requestContext, final DatabaseInfo databaseInfo) {
        final QualifiedName databaseName = databaseInfo.getName();
        // TODO: handling for more exception types like DatabaseAlreadyExistsException once data model supports this
        try {
            this.polarisConnector.createDatabase(databaseName.getDatabaseName());
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed creating polaris database %s", databaseName), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void delete(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            // TODO: implement once data model supports this operation
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed deleting polaris database %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void update(final ConnectorRequestContext context, final DatabaseInfo databaseInfo) {
        try {
            // TODO: implement once data model supports this operation
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed updating polaris database %s", databaseInfo.getName()), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public DatabaseInfo get(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            // TODO: implement once data model supports this operation
            return null;
        } catch (Exception exception) {
            throw new ConnectorException(
                String.format("Failed get polaris database %s", name), exception);
        }
    }
}
