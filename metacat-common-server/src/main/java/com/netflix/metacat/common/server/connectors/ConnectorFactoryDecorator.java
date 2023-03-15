package com.netflix.metacat.common.server.connectors;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A decorator for a connector factory to add additional cross-cutting functionality
 * to all connector services.
 */
@Slf4j
public class ConnectorFactoryDecorator implements ConnectorFactory {
    @Getter
    private final ConnectorFactory delegate;
    private final Supplier<ConnectorCatalogService> catalogService;
    private final Supplier<ConnectorDatabaseService> databaseService;
    private final Supplier<ConnectorTableService> tableService;
    private final Supplier<ConnectorPartitionService> partitionService;

    /**
     * Creates the decorated connector factory that wraps connector services
     * with additional wrappers.
     *
     * @param connectorPlugin the underlying plugin
     * @param connectorContext the connector context for the underlying plugin
     */
    public ConnectorFactoryDecorator(@NonNull final ConnectorPlugin connectorPlugin,
                                     @NonNull final ConnectorContext connectorContext) {
        this.delegate = connectorPlugin.create(connectorContext);

        if (isRateLimiterEnabled(connectorContext)) {
            log.info("Creating rate-limited connector services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());

            final RateLimiter rateLimiter = connectorContext.getApplicationContext().getBean(RateLimiter.class);

            // not all connectors implement these methods;
            // calling these eagerly may throw an unsupported operation exception;
            // hence we only memoize these for now and call them at runtime when they are needed
            catalogService = memoize(
                () -> new ThrottlingConnectorCatalogService(delegate.getCatalogService(), rateLimiter));
            databaseService = memoize(
                () -> new ThrottlingConnectorDatabaseService(delegate.getDatabaseService(), rateLimiter));
            tableService = memoize(
                () -> new ThrottlingConnectorTableService(delegate.getTableService(), rateLimiter));
            partitionService = memoize(
                () -> new ThrottlingConnectorPartitionService(delegate.getPartitionService(), rateLimiter));
        } else {
            log.info("Creating non rate-limited connector services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());

            catalogService = memoize(delegate::getCatalogService);
            databaseService = memoize(delegate::getDatabaseService);
            tableService = memoize(delegate::getTableService);
            partitionService = memoize(delegate::getPartitionService);
        }
    }

    @Override
    public ConnectorCatalogService getCatalogService() {
        return catalogService.get();
    }

    @Override
    public ConnectorDatabaseService getDatabaseService() {
        return databaseService.get();
    }

    @Override
    public ConnectorTableService getTableService() {
        return tableService.get();
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        return partitionService.get();
    }

    @Override
    public String getCatalogName() {
        return delegate.getCatalogName();
    }

    @Override
    public String getCatalogShardName() {
        return delegate.getCatalogShardName();
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    private <T> Supplier<T> memoize(final Supplier<T> supplier) {
        return Suppliers.memoize(supplier::get)::get;
    }

    private boolean isRateLimiterEnabled(final ConnectorContext connectorContext) {
        if (connectorContext.getConfiguration() == null) {
            return true;
        }

        return !Boolean.parseBoolean(
            connectorContext.getConfiguration().getOrDefault("connector.rate-limiter-exempted", "false")
        );
    }
}
