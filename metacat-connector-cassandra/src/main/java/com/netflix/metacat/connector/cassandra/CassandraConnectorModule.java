/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.metacat.connector.cassandra;

import com.datastax.driver.core.Cluster;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * A Guice Module for the CassandraConnector.
 *
 * @author tgianos
 * @since 1.0.0
 */
public class CassandraConnectorModule extends AbstractModule {

    private static final String CONTACT_POINTS_KEY = "cassandra.contactPoints";
    private static final String PORT_KEY = "cassandra.port";
    private static final String USERNAME_KEY = "cassandra.username";
    private static final String PASSWORD_KEY = "cassandra.password";

    private final String catalogShardName;
    private final Map<String, String> configuration;

    /**
     * Constructor.
     *
     * @param catalogShardName  catalog shard name
     * @param configuration     connector configuration
     */
    CassandraConnectorModule(
        @Nonnull @NonNull final String catalogShardName,
        @Nonnull @NonNull final Map<String, String> configuration
    ) {
        this.catalogShardName = catalogShardName;
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        this.bind(CassandraTypeConverter.class).toInstance(new CassandraTypeConverter());
        this.bind(CassandraExceptionMapper.class).toInstance(new CassandraExceptionMapper());
        this.bind(ConnectorDatabaseService.class)
            .to(ConnectorUtils.getDatabaseServiceClass(this.configuration, CassandraConnectorDatabaseService.class))
            .in(Scopes.SINGLETON);
        this.bind(ConnectorTableService.class)
            .to(ConnectorUtils.getTableServiceClass(this.configuration, CassandraConnectorTableService.class))
            .in(Scopes.SINGLETON);
        this.bind(ConnectorPartitionService.class)
            .to(ConnectorUtils.getPartitionServiceClass(this.configuration, CassandraConnectorPartitionService.class))
            .in(Scopes.SINGLETON);
    }

    /**
     * Creates a cluster to use for connecting to Cassandra.
     *
     * @return The cluster singleton to use within the Injector
     */
    @Provides
    @Singleton
    Cluster provideCluster() {
        final Cluster.Builder builder = Cluster.builder().withClusterName(this.catalogShardName);

        // Contact points are required
        final String contactPointsString = this.configuration.get(CONTACT_POINTS_KEY);
        if (contactPointsString == null) {
            throw new IllegalArgumentException(CONTACT_POINTS_KEY + " value is missing and is required.");
        }
        final String[] contactPoints = contactPointsString.split(",");
        final ImmutableList.Builder<InetAddress> contactAddresses = ImmutableList.builder();
        for (final String contactPoint : contactPoints) {
            try {
                contactAddresses.add(InetAddress.getByName(contactPoint));
            } catch (final UnknownHostException uhe) {
                throw new IllegalArgumentException("Can't parse contact point " + contactPoint, uhe);
            }
        }
        builder.addContactPoints(contactAddresses.build());
        final String port = this.configuration.get(PORT_KEY);
        if (port != null) {
            builder.withPort(Integer.parseInt(port));
        }

        final String username = this.configuration.get(USERNAME_KEY);
        final String password = this.configuration.get(PASSWORD_KEY);
        if (username != null && password != null) {
            builder.withCredentials(username, password);
        }
        return builder.build();
    }
}
