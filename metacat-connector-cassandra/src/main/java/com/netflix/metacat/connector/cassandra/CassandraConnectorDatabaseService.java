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
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import jakarta.inject.Inject;
import java.util.Comparator;
import java.util.List;

/**
 * Implementation of the database service for Cassandra. For Cassandra the {@code Keyspace} is the equivalent of a JDBC
 * database.
 *
 * @author tgianos
 * @see ConnectorDatabaseService
 * @since 1.0.0
 */
@Slf4j
public class CassandraConnectorDatabaseService extends CassandraService implements ConnectorDatabaseService {

    /**
     * Constructor.
     *
     * @param cluster         The cassandra cluster connection to use
     * @param exceptionMapper The exception mapper to use to convert from DriverException to ConnectorException
     */
    @Inject
    public CassandraConnectorDatabaseService(
        @Nonnull @NonNull final Cluster cluster,
        @Nonnull @NonNull final CassandraExceptionMapper exceptionMapper
    ) {
        super(cluster, exceptionMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create(
        @Nonnull @NonNull final ConnectorRequestContext context,
        @Nonnull @NonNull final DatabaseInfo resource
    ) {
        final String keyspace = resource.getName().getDatabaseName();
        log.debug("Attempting to create a Cassandra Keyspace named {} for request {}", keyspace, context);
        try {
            // TODO: Make this take parameters for replication and the class
            this.executeQuery(
                "CREATE KEYSPACE "
                    + keyspace
                    + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"
            );
            log.debug("Successfully created Cassandra Keyspace named {} for request {}", keyspace, context);
        } catch (final DriverException de) {
            log.error(de.getMessage(), de);
            throw this.getExceptionMapper().toConnectorException(de, resource.getName());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@Nonnull @NonNull final ConnectorRequestContext context,
                       @Nonnull @NonNull final QualifiedName name) {
        final String keyspace = name.getDatabaseName();
        log.debug("Attempting to drop Cassandra keyspace {} for request {}", keyspace, context);
        try {
            this.executeQuery("DROP KEYSPACE IF EXISTS " + keyspace + ";");
            log.debug("Successfully dropped {} keyspace", keyspace);
        } catch (final DriverException de) {
            log.error(de.getMessage(), de);
            throw this.getExceptionMapper().toConnectorException(de, name);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseInfo get(
        @Nonnull @NonNull final ConnectorRequestContext context,
        @Nonnull @NonNull final QualifiedName name
    ) {
        final String keyspace = name.getDatabaseName();
        log.debug("Attempting to get keyspace metadata for keyspace {} for request {}", keyspace, context);
        try {
            final KeyspaceMetadata keyspaceMetadata = this.getCluster().getMetadata().getKeyspace(keyspace);
            if (keyspaceMetadata == null) {
                throw new DatabaseNotFoundException(name);
            }

            log.debug("Successfully found the keyspace metadata for {} for request {}", name, context);
            return DatabaseInfo.builder().name(name).build();
        } catch (final DriverException de) {
            log.error(de.getMessage(), de);
            throw this.getExceptionMapper().toConnectorException(de, name);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> listViewNames(
        @Nonnull @NonNull final ConnectorRequestContext context,
        @Nonnull @NonNull final QualifiedName databaseName
    ) {
        final String catalogName = databaseName.getCatalogName();
        final String keyspace = databaseName.getDatabaseName();
        log.debug("Attempting to get materialized view names for keyspace {} due to request {}", keyspace, context);
        try {
            final KeyspaceMetadata keyspaceMetadata = this.getCluster().getMetadata().getKeyspace(keyspace);
            if (keyspaceMetadata == null) {
                throw new DatabaseNotFoundException(databaseName);
            }

            final ImmutableList.Builder<QualifiedName> viewsBuilder = ImmutableList.builder();
            for (final MaterializedViewMetadata view : keyspaceMetadata.getMaterializedViews()) {
                viewsBuilder.add(
                    QualifiedName.ofView(catalogName, keyspace, view.getBaseTable().getName(), view.getName())
                );
            }

            final List<QualifiedName> views = viewsBuilder.build();
            log.debug("Successfully found {} views for keyspace {} due to request {}", views.size(), keyspace, context);
            return views;
        } catch (final DriverException de) {
            log.error(de.getMessage(), de);
            throw this.getExceptionMapper().toConnectorException(de, databaseName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(
        @Nonnull @NonNull final ConnectorRequestContext context,
        @Nonnull @NonNull final QualifiedName name
    ) {
        final String keyspace = name.getDatabaseName();
        log.debug("Checking if keyspace {} exists for request {}", keyspace, context);
        try {
            final boolean exists = this.getCluster().getMetadata().getKeyspace(keyspace) != null;
            log.debug("Keyspace {} {} for request {}", keyspace, exists ? "exists" : "doesn't exist", context);
            return exists;
        } catch (final DriverException de) {
            log.error(de.getMessage(), de);
            throw this.getExceptionMapper().toConnectorException(de, name);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DatabaseInfo> list(
        @Nonnull @NonNull final ConnectorRequestContext context,
        @Nonnull @NonNull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        log.debug("Attempting to list keyspaces for request {}", context);
        final ImmutableList.Builder<DatabaseInfo> keyspacesBuilder = ImmutableList.builder();
        for (final QualifiedName keyspace : this.listNames(context, name, prefix, sort, pageable)) {
            keyspacesBuilder.add(DatabaseInfo.builder().name(keyspace).build());
        }
        final List<DatabaseInfo> keyspaces = keyspacesBuilder.build();
        log.debug("Successfully listed {} keyspaces for request {}", keyspaces.size(), context);
        return keyspaces;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> listNames(
        @Nonnull @NonNull final ConnectorRequestContext context,
        @Nonnull @NonNull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        log.debug("Attempting to list keyspaces for request {}", context);
        try {
            final List<QualifiedName> names = Lists.newArrayList();
            for (final KeyspaceMetadata keyspace : this.getCluster().getMetadata().getKeyspaces()) {
                final String keyspaceName = keyspace.getName();
                if (prefix != null && !keyspaceName.startsWith(prefix.getDatabaseName())) {
                    continue;
                }
                names.add(QualifiedName.ofDatabase(name.getCatalogName(), keyspaceName));
            }

            if (sort != null) {
                // We can only really sort by the database name at this level so ignore SortBy field
                final Comparator<QualifiedName> comparator = Comparator.comparing(QualifiedName::getDatabaseName);
                ConnectorUtils.sort(names, sort, comparator);
            }

            final List<QualifiedName> results = ConnectorUtils.paginate(names, pageable);
            log.debug("Finished listing keyspaces for request {}", context);
            return results;
        } catch (final DriverException de) {
            log.error(de.getMessage(), de);
            throw this.getExceptionMapper().toConnectorException(de, name);
        }
    }
}
