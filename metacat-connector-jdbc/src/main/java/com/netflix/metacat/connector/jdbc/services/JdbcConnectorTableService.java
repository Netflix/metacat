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
package com.netflix.metacat.connector.jdbc.services;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.Type;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Generic JDBC implementation of the ConnectorTableService.
 *
 * @author tgianos
 * @since 0.1.52
 */
@Slf4j
public class JdbcConnectorTableService implements ConnectorTableService {

    private final DataSource dataSource;

    /**
     * Constructor.
     *
     * @param dataSource the datasource to use to connect to the database
     */
    public JdbcConnectorTableService(@Nonnull final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        final String databaseName = name.getDatabaseName();
        final String tableName = name.getTableName();
        log.debug("Attempting to delete table {} from database {} for request {}", tableName, databaseName, context);
        try (final Connection connection = this.dataSource.getConnection()) {
            final boolean upperCase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (upperCase) {
                connection.setSchema(databaseName.toUpperCase(Locale.ENGLISH));
            } else {
                connection.setSchema(databaseName);
            }
            final String finalTableName = upperCase ? tableName.toUpperCase(Locale.ENGLISH) : tableName;
            final String sql = "DROP TABLE " + finalTableName + ";";
            JdbcConnectorUtils.executeUpdate(connection, sql);
            log.debug("Deleted table {} from database {} for request {}", tableName, databaseName, context);
        } catch (final SQLException se) {
            throw Throwables.propagate(se);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableInfo get(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        log.debug("Beginning to get table metadata for qualified name {} for request {}", name, context);

        try (final Connection connection = this.dataSource.getConnection()) {
            final String database = name.getDatabaseName();
            connection.setSchema(database);
            final DatabaseMetaData metaData = connection.getMetaData();
            final String escapeString = metaData.getSearchStringEscape();

            final ImmutableList.Builder<FieldInfo> fields = ImmutableList.builder();
            final ResultSet columns
                = metaData.getColumns(connection.getCatalog(), database, name.getTableName(), escapeString);
            while (columns.next()) {
                final FieldInfo.Builder builder = new FieldInfo.Builder(
                    columns.getString("COLUMN_NAME"),
                    columns.getString("TYPE_NAME"),
                    this.toMetacatType(columns.getInt("DATA_TYPE"))
                );
                builder.withComment(columns.getString("REMARKS"));
                builder.withPos(columns.getInt("ORDINAL_POSITION"));
                builder.withNullable(columns.getString("IS_NULLABLE").equals("YES"));
                builder.withSize(columns.getInt("COLUMN_SIZE"));
                builder.withDefaultValue(columns.getString("COLUMN_DEF"));
                fields.add(builder.build());
            }
            log.debug("Finished getting table metadata for qualified name {} for request {}", name, context);
            return new TableInfo.Builder(name, fields.build()).build();
        } catch (final SQLException se) {
            throw Throwables.propagate(se);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<TableInfo> list(
        @Nonnull final ConnectorContext context,
        @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        log.debug("Beginning to list table metadata for {} for request {}", name, context);

        final ImmutableList.Builder<TableInfo> builder = ImmutableList.builder();
        for (final QualifiedName tableName : this.listNames(context, name, prefix, sort, pageable)) {
            builder.add(this.get(context, tableName));
        }
        log.debug("Finished listing table metadata for {} for request {}", name, context);
        return builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> listNames(
        @Nonnull final ConnectorContext context,
        @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        log.debug("Beginning to list tables names for qualified name {} for request {}", name, context);

        try (final Connection connection = this.dataSource.getConnection()) {
            final String catalog = name.getCatalogName();
            final String database = name.getDatabaseName();
            connection.setSchema(database);
            final DatabaseMetaData metaData = connection.getMetaData();
            final String escapeString = metaData.getSearchStringEscape();
            final List<QualifiedName> names = Lists.newArrayList();
            final ResultSet tables;
            if (prefix == null || StringUtils.isEmpty(prefix.getTableName())) {
                tables = metaData.getTables(connection.getCatalog(), database, escapeString, null);
            } else {
                tables = metaData.getTables(
                    connection.getCatalog(),
                    database,
                    prefix.getTableName() + escapeString,
                    null
                );
            }

            while (tables.next()) {
                names.add(QualifiedName.ofTable(catalog, database, tables.getString("TABLE_NAME")));
            }

            // Does user want sorting?
            if (sort != null) {
                final Comparator<QualifiedName> comparator = Comparator.comparing(QualifiedName::getTableName);
                JdbcConnectorUtils.sort(names, sort, comparator);
            }

            // Does user want pagination?
            final List<QualifiedName> results = JdbcConnectorUtils.paginate(names, pageable);

            log.debug("Finished listing tables names for qualified name {} for request {}", name, context);
            return results;
        } catch (final SQLException se) {
            throw Throwables.propagate(se);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rename(
        @Nonnull final ConnectorContext context,
        @Nonnull final QualifiedName oldName,
        @Nonnull final QualifiedName newName
    ) {
        final String oldDatabaseName = oldName.getDatabaseName();
        final String newDatabaseName = newName.getDatabaseName();
        final String oldTableName = oldName.getTableName();
        final String newTableName = newName.getTableName();
        log.debug(
            "Attempting to re-name table {}/{} to {}/{} for request {}",
            oldDatabaseName,
            oldTableName,
            newDatabaseName,
            newTableName,
            context
        );

        if (!oldDatabaseName.equals(newDatabaseName)) {
            throw new IllegalArgumentException(
                "Database names must match and they are " + oldDatabaseName + " and " + newDatabaseName
            );
        }
        try (final Connection connection = this.dataSource.getConnection()) {
            final boolean upperCase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (upperCase) {
                connection.setSchema(oldDatabaseName.toUpperCase(Locale.ENGLISH));
            } else {
                connection.setSchema(oldDatabaseName);
            }
            final String finalOldTableName = upperCase ? oldTableName.toUpperCase(Locale.ENGLISH) : oldTableName;
            final String finalNewTableName = upperCase ? newTableName.toUpperCase(Locale.ENGLISH) : newTableName;
            final String sql = "ALTER TABLE " + finalOldTableName + " RENAME " + finalNewTableName + ";";
            JdbcConnectorUtils.executeUpdate(connection, sql);
            log.debug(
                "Renamed table {}/{} to {}/{} for request {}",
                oldDatabaseName,
                oldTableName,
                newDatabaseName,
                newTableName,
                context
            );
        } catch (final SQLException se) {
            throw Throwables.propagate(se);
        }
    }

    private Type toMetacatType(final int jdbcType) {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BaseType.BOOLEAN;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return BaseType.BIGINT;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BaseType.DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return BaseType.STRING;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return BaseType.VARBINARY;
            case Types.DATE:
                return BaseType.DATE;
            case Types.TIME:
                return BaseType.TIME;
            case Types.TIMESTAMP:
                return BaseType.TIMESTAMP;
            default:
                throw new IllegalArgumentException("Unmapped SQL type " + jdbcType);
        }
    }
}
