package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.server.api.authorization.Authorization;
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
    private final ConnectorPlugin connectorPlugin;
    @Getter
    private final ConnectorFactory delegate;
    private final ConnectorContext connectorContext;
    private final RateLimiter rateLimiter;
    private final boolean rateLimiterEnabled;
    private final Authorization authorization;
    private final boolean authorizationEnabled;

    /**
     * Creates the decorated connector factory that wraps connector services
     * with additional wrappers.
     *
     * @param connectorPlugin the underlying plugin
     * @param connectorContext the connector context for the underlying plugin
     */
    public ConnectorFactoryDecorator(@NonNull final ConnectorPlugin connectorPlugin,
                                     @NonNull final ConnectorContext connectorContext) {
        this.connectorPlugin = connectorPlugin;
        this.delegate = connectorPlugin.create(connectorContext);
        this.connectorContext = connectorContext;

        this.rateLimiter = connectorContext.getApplicationContext().getBean(RateLimiter.class);
        this.authorization = connectorContext.getApplicationContext().getBean(Authorization.class);

        // we can cache these configs at startup since this is the connector level config
        // that does not change later. Actual decision to enable and enforce throttling and/or
        // authorization is in the rate limiter or auth implementations which are more dynamic
        // and accommodate changes from the Metacat dynamic configuration.
        this.rateLimiterEnabled = isRateLimiterEnabled();
        this.authorizationEnabled = isAuthorizationEnabled();
    }

    @Override
    public ConnectorCatalogService getCatalogService() {
        ConnectorCatalogService service = delegate.getCatalogService();

        if (authorizationEnabled) {
            log.info("Creating auth enabled connector catalog services for connector-type: {}, "
                    + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new AuthEnabledConnectorCatalogService(service, authorization);
        }

        if (rateLimiterEnabled) {
            log.info("Creating rate-limited connector catalog services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorCatalogService(service, rateLimiter);
        }

        return service;
    }

    @Override
    public ConnectorDatabaseService getDatabaseService() {
        ConnectorDatabaseService service = delegate.getDatabaseService();

        if (authorizationEnabled) {
            log.info("Creating auth enabled connector database services for connector-type: {}, "
                    + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new AuthEnabledConnectorDatabaseService(service, authorization);
        }

        if (rateLimiterEnabled) {
            log.info("Creating rate-limited connector database services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorDatabaseService(service, rateLimiter);
        }

        return service;
    }

    @Override
    public ConnectorTableService getTableService() {
        ConnectorTableService service = delegate.getTableService();

        if (authorizationEnabled) {
            log.info("Creating auth enabled connector table services for connector-type: {}, "
                    + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new AuthEnabledConnectorTableService(service, authorization);
        }

        if (rateLimiterEnabled) {
            log.info("Creating rate-limited connector table services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorTableService(service, rateLimiter);
        }

        return service;
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        ConnectorPartitionService service = delegate.getPartitionService();

        if (authorizationEnabled) {
            log.info("Creating auth enabled connector partition services for connector-type: {}, "
                    + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new AuthEnabledConnectorPartitionService(service, authorization);
        }

        if (rateLimiterEnabled) {
            log.info("Creating rate-limited connector partition services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
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

    private boolean isAuthorizationEnabled() {
        if (connectorContext.getConfiguration() == null) {
            return true;
        }

        return !Boolean.parseBoolean(
            connectorContext.getConfiguration().getOrDefault("connector.authorization-exempted", "false")
        );
    }
}
