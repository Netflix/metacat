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
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Comparator;
import java.util.List;

/**
 * Cassandra implementation of the ConnectorTableService.
 *
 * @author tgianos
 * @see ConnectorTableService
 * @since 3.0.0
 */
@Slf4j
public class CassandraConnectorTableService extends CassandraService implements ConnectorTableService {

    private final CassandraTypeConverter typeConverter;

    /**
     * Constructor.
     *
     * @param cluster       The cluster this service connects to
     * @param typeConverter The type converter to convert from CQL types to Metacat Types
     */
    @Inject
    public CassandraConnectorTableService(
        @Nonnull @NonNull final Cluster cluster,
        @Nonnull @NonNull final CassandraTypeConverter typeConverter
    ) {
        super(cluster);
        this.typeConverter = typeConverter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@Nonnull @NonNull final ConnectorContext context, @Nonnull @NonNull final QualifiedName name) {
        final String keyspace = name.getDatabaseName();
        final String table = name.getTableName();
        log.debug("Attempting to delete Cassandra table {}.{} for request {}", keyspace, table, context);
        this.executeQuery("USE " + keyspace + "; DROP TABLE IF EXISTS " + table + ";");
        log.debug("Successfully deleted Cassandra table {}.{} for request {}", keyspace, table, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableInfo get(@Nonnull @NonNull final ConnectorContext context, @Nonnull @NonNull final QualifiedName name) {
        final String keyspace = name.getDatabaseName();
        final String table = name.getTableName();
        log.debug("Attempting to get metadata for Cassandra table {}.{} for request {}", keyspace, table, context);
        final KeyspaceMetadata keyspaceMetadata = this.getCluster().getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            throw new DatabaseNotFoundException(name);
        }
        final TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
        if (tableMetadata == null) {
            throw new TableNotFoundException(name);
        }

        final TableInfo tableInfo = this.getTableInfo(name, tableMetadata);
        log.debug("Successfully got metadata for Cassandra table {}.{} for request {}", keyspace, table, context);
        return tableInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(
        @Nonnull @NonNull final ConnectorContext context,
        @Nonnull @NonNull final QualifiedName name
    ) {
        final String keyspace = name.getDatabaseName();
        final String table = name.getTableName();
        log.debug("Checking if Cassandra table {}.{} exists for request {}", keyspace, table, context);
        final KeyspaceMetadata keyspaceMetadata = this.getCluster().getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            return false;
        }

        final boolean exists = keyspaceMetadata.getTable(table) != null;
        log.debug(
            "Cassandra table {}.{} {} for request {}",
            keyspace,
            table,
            exists ? "exists" : "doesn't exist",
            context
        );

        return exists;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<TableInfo> list(
        @Nonnull @NonNull final ConnectorContext context,
        @Nonnull @NonNull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        final String keyspace = name.getDatabaseName();
        log.debug("Attempting to list tables in Cassandra keyspace {} for request {}", keyspace, context);
        final KeyspaceMetadata keyspaceMetadata = this.getCluster().getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            throw new DatabaseNotFoundException(name);
        }

        // TODO: Should we include views?
        final List<TableInfo> tables = Lists.newArrayList();
        for (final TableMetadata tableMetadata : keyspaceMetadata.getTables()) {
            if (prefix != null && !tableMetadata.getName().startsWith(prefix.getTableName())) {
                continue;
            }
            tables.add(this.getTableInfo(name, tableMetadata));
        }

        // Sort
        if (sort != null) {
            final Comparator<TableInfo> tableComparator = Comparator.comparing((t) -> t.getName().getTableName());
            ConnectorUtils.sort(tables, sort, tableComparator);
        }

        // Paging
        final List<TableInfo> pagedTables = ConnectorUtils.paginate(tables, pageable);

        log.debug("Listed {} tables in Cassandra keyspace {} for request {}", pagedTables.size(), keyspace, context);
        return pagedTables;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> listNames(
        @Nonnull @NonNull final ConnectorContext context,
        @Nonnull @NonNull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        final String catalog = name.getCatalogName();
        final String keyspace = name.getDatabaseName();
        log.debug("Attempting to list table names in Cassandra keyspace {} for request {}", keyspace, context);
        final KeyspaceMetadata keyspaceMetadata = this.getCluster().getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            throw new DatabaseNotFoundException(name);
        }

        // TODO: Should we include views?
        final List<QualifiedName> tableNames = Lists.newArrayList();
        for (final TableMetadata tableMetadata : keyspaceMetadata.getTables()) {
            final String tableName = tableMetadata.getName();
            if (prefix != null && !tableName.startsWith(prefix.getTableName())) {
                continue;
            }
            tableNames.add(QualifiedName.ofTable(catalog, keyspace, tableName));
        }

        // Sort
        if (sort != null) {
            final Comparator<QualifiedName> tableNameComparator = Comparator.comparing(QualifiedName::getTableName);
            ConnectorUtils.sort(tableNames, sort, tableNameComparator);
        }

        // Paging
        final List<QualifiedName> paged = ConnectorUtils.paginate(tableNames, pageable);

        log.debug("Listed {} table names in Cassandra keyspace {} for request {}", paged.size(), keyspace, context);
        return paged;
    }

    private TableInfo getTableInfo(
        @Nonnull @NonNull final QualifiedName name,
        @Nonnull @NonNull final TableMetadata tableMetadata
    ) {
        final ImmutableList.Builder<FieldInfo> fieldInfoBuilder = ImmutableList.builder();
        // TODO: Ignores clustering, primary key, index, etc columns. We need to rework TableInfo to support
        for (final ColumnMetadata column : tableMetadata.getColumns()) {
            final String dataType = column.getType().toString();
            fieldInfoBuilder.add(
                FieldInfo.builder()
                    .name(column.getName())
                    .sourceType(dataType)
                    .type(this.typeConverter.toMetacatType(dataType))
                    .build()
            );
        }
        return TableInfo.builder()
            .name(QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(), tableMetadata.getName()))
            .fields(fieldInfoBuilder.build())
            .build();
    }
}
