package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A decorator for a connector factory to add additional cross-cutting functionality
 * to all connector services.
 */
@Slf4j
@RequiredArgsConstructor
public class ConnectorFactoryDecorator implements ConnectorFactory {
    private final ConnectorPlugin connectorPlugin;
    @Getter
    private final ConnectorFactory delegate;
    private final ConnectorContext connectorContext;
    private final RateLimiter rateLimiter;

    @Override
    public ConnectorCatalogService getCatalogService() {
        ConnectorCatalogService service = delegate.getCatalogService();

        if (isRateLimiterEnabled()) {
            log.info("Creating rate-limited connector catalog services for connector-type: {}, " +
                         "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorCatalogService(service, rateLimiter);
        }

        return service;
    }

    @Override
    public ConnectorDatabaseService getDatabaseService() {
        ConnectorDatabaseService service = delegate.getDatabaseService();

        if (isRateLimiterEnabled()) {
            log.info("Creating rate-limited connector database services for connector-type: {}, " +
                         "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorDatabaseService(service, rateLimiter);
        }

        return service;
    }

    @Override
    public ConnectorTableService getTableService() {
        ConnectorTableService service = delegate.getTableService();

        if (isRateLimiterEnabled()) {
            log.info("Creating rate-limited connector table services for connector-type: {}, " +
                         "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorTableService(service, rateLimiter);
        }

        return service;
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        ConnectorPartitionService service = delegate.getPartitionService();

        if (isRateLimiterEnabled()) {
            log.info("Creating rate-limited connector partition services for connector-type: {}, " +
                         "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorPartitionService(service, rateLimiter);
        }

        return service;
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

    private boolean isRateLimiterEnabled() {
        if (connectorContext.getConfiguration() == null) {
            return true;
        }

        return !Boolean.parseBoolean(
            connectorContext.getConfiguration().getOrDefault("connector.rate-limiter-exempted", "false")
        );
    }
}
