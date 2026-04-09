/*
 *
 *  Copyright 2024 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.server.api.ratelimiter.RateLimiter;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A decorator for a connector factory to add additional cross-cutting functionality
 * to all connector services.
 */
@Slf4j
public class ConnectorFactoryDecorator implements ConnectorFactory {
    /**
     * Configuration key to require authorization for catalog access.
     */
    public static final String CONFIG_AUTHORIZATION_REQUIRED = "connector.authorization-required";

    /**
     * Configuration key for comma-separated list of authorized callers.
     */
    public static final String CONFIG_AUTHORIZED_CALLERS = "connector.authorized-callers";

    /**
     * Configuration key to lock table location updates for a catalog.
     */
    public static final String CONFIG_LOCATION_UPDATE_LOCKED = "connector.location-update-locked";

    /**
     * Configuration key to exempt a connector from rate limiting.
     */
    public static final String CONFIG_RATE_LIMITER_EXEMPTED = "connector.rate-limiter-exempted";

    private final ConnectorPlugin connectorPlugin;
    @Getter
    private final ConnectorFactory delegate;
    private final ConnectorContext connectorContext;
    private final RateLimiter rateLimiter;
    private final boolean rateLimiterEnabled;
    private final boolean authorizationRequired;
    private final Set<String> authorizedCallers;
    private final boolean locationUpdateLocked;

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

        // we can cache this config at startup since this is the connector level config
        // that does not change later. Actual decision to enable and enforce throttling
        // is in the rate limiter implementation which is more dynamic and accommodates
        // changes from the Metacat dynamic configuration.
        this.rateLimiterEnabled = isRateLimiterEnabled();
        this.authorizationRequired = isAuthorizationRequired();
        this.authorizedCallers = getAuthorizedCallers();
        this.locationUpdateLocked = isLocationUpdateLocked();

        if (this.authorizationRequired) {
            log.info("Authorization enabled for catalog {} with allowed callers: {}",
                connectorContext.getCatalogName(), this.authorizedCallers);
        }

        if (this.locationUpdateLocked) {
            log.info("Location update lock enabled for catalog {}",
                connectorContext.getCatalogName());
        }
    }

    @Override
    public ConnectorCatalogService getCatalogService() {
        ConnectorCatalogService service = delegate.getCatalogService();

        // Apply throttling first (innermost decorator)
        if (rateLimiterEnabled) {
            log.info("Creating rate-limited connector catalog services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorCatalogService(service, rateLimiter);
        }

        // Apply authorization second (outermost decorator - checked first)
        if (authorizationRequired) {
            log.info("Creating authorized connector catalog services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new AuthorizingConnectorCatalogService(
                service, authorizedCallers, connectorContext.getCatalogName());
        }

        return service;
    }

    @Override
    public ConnectorDatabaseService getDatabaseService() {
        ConnectorDatabaseService service = delegate.getDatabaseService();

        // Apply throttling first (innermost decorator)
        if (rateLimiterEnabled) {
            log.info("Creating rate-limited connector database services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorDatabaseService(service, rateLimiter);
        }

        // Apply authorization second (outermost decorator - checked first)
        if (authorizationRequired) {
            log.info("Creating authorized connector database services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new AuthorizingConnectorDatabaseService(
                service, authorizedCallers, connectorContext.getCatalogName());
        }

        return service;
    }

    @Override
    public ConnectorTableService getTableService() {
        ConnectorTableService service = delegate.getTableService();

        // Apply throttling first (innermost decorator)
        if (rateLimiterEnabled) {
            log.info("Creating rate-limited connector table services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorTableService(service, rateLimiter);
        }

        // Apply authorization and/or location lock (outermost decorator - checked first)
        if (authorizationRequired || locationUpdateLocked) {
            log.info("Creating authorized connector table services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}, auth: {}, locationLock: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName(),
                authorizationRequired, locationUpdateLocked);
            service = new AuthorizingConnectorTableService(
                service, authorizedCallers, connectorContext.getCatalogName(),
                authorizationRequired, locationUpdateLocked);
        }

        return service;
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        ConnectorPartitionService service = delegate.getPartitionService();

        // Apply throttling first (innermost decorator)
        if (rateLimiterEnabled) {
            log.info("Creating rate-limited connector partition services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new ThrottlingConnectorPartitionService(service, rateLimiter);
        }

        // Apply authorization second (outermost decorator - checked first)
        if (authorizationRequired) {
            log.info("Creating authorized connector partition services for connector-type: {}, "
                         + "plugin-type: {}, catalog: {}, shard: {}",
                connectorContext.getConnectorType(), connectorPlugin.getType(),
                connectorContext.getCatalogName(), connectorContext.getCatalogShardName());
            service = new AuthorizingConnectorPartitionService(
                service, authorizedCallers, connectorContext.getCatalogName());
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
            connectorContext.getConfiguration().getOrDefault(CONFIG_RATE_LIMITER_EXEMPTED, "false")
        );
    }

    private boolean isLocationUpdateLocked() {
        if (connectorContext.getConfiguration() == null) {
            return false;
        }

        return Boolean.parseBoolean(
            connectorContext.getConfiguration().getOrDefault(CONFIG_LOCATION_UPDATE_LOCKED, "false")
        );
    }

    private boolean isAuthorizationRequired() {
        if (connectorContext.getConfiguration() == null) {
            return false;
        }

        return Boolean.parseBoolean(
            connectorContext.getConfiguration().getOrDefault(CONFIG_AUTHORIZATION_REQUIRED, "false")
        );
    }

    private Set<String> getAuthorizedCallers() {
        if (connectorContext.getConfiguration() == null) {
            return Collections.emptySet();
        }

        final String callers = connectorContext.getConfiguration()
            .getOrDefault(CONFIG_AUTHORIZED_CALLERS, "");

        if (callers.isEmpty()) {
            return Collections.emptySet();
        }

        // Split by comma, trim whitespace, filter empty strings, collect to set
        // Note: matching is case-sensitive (e.g., "IRC" will not match "irc")
        return Arrays.stream(callers.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toSet());
    }
}
