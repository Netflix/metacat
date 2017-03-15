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
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Generic JDBC implementation of the ConnectorDatabaseService.
 *
 * @author tgianos
 * @since 1.0.0
 */
@Slf4j
@Getter
public class JdbcConnectorDatabaseService implements ConnectorDatabaseService {

    private final DataSource dataSource;

    /**
     * Constructor.
     *
     * @param dataSource The jdbc datasource instance to use to make connections
     */
    @Inject
    public JdbcConnectorDatabaseService(@Nonnull @NonNull final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create(@Nonnull final ConnectorContext context, @Nonnull final DatabaseInfo resource) {
        final String databaseName = resource.getName().getDatabaseName();
        log.debug("Beginning to create database {} for request {}", databaseName, context);
        try (final Connection connection = this.dataSource.getConnection()) {
            JdbcConnectorUtils.executeUpdate(connection, "CREATE DATABASE " + databaseName);
            log.debug("Finished creating database {} for request {}", databaseName, context);
        } catch (final SQLException se) {
            throw Throwables.propagate(se);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        final String databaseName = name.getDatabaseName();
        log.debug("Beginning to drop database {} for request {}", databaseName, context);
        try (final Connection connection = this.dataSource.getConnection()) {
            JdbcConnectorUtils.executeUpdate(connection, "DROP DATABASE " + databaseName);
            log.debug("Finished dropping database {} for request {}", databaseName, context);
        } catch (final SQLException se) {
            throw Throwables.propagate(se);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseInfo get(@Nonnull final ConnectorContext context, @Nonnull final QualifiedName name) {
        final String databaseName = name.getDatabaseName();
        log.debug("Beginning to get database metadata for {} for request {}", databaseName, context);
        return DatabaseInfo.builder().name(name).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DatabaseInfo> list(
        @Nonnull final ConnectorContext context,
        @Nonnull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        final String catalogName = name.getCatalogName();
        log.debug("Beginning to list database metadata for catalog {} for request {}", catalogName, context);

        final ImmutableList.Builder<DatabaseInfo> builder = ImmutableList.builder();
        for (final QualifiedName dbName : this.listNames(context, name, prefix, sort, pageable)) {
            builder.add(this.get(context, dbName));
        }
        log.debug("Finished listing database metadata for catalog {} for request {}", catalogName, context);
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
        final String catalogName = name.getCatalogName();
        log.debug("Beginning to list database names for catalog {} for request {}", catalogName, context);

        try (final Connection connection = this.dataSource.getConnection()) {
            final DatabaseMetaData metaData = connection.getMetaData();
            final List<QualifiedName> names = Lists.newArrayList();

            try (final ResultSet schemas = prefix == null || StringUtils.isEmpty(prefix.getDatabaseName())
                ? metaData.getSchemas()
                : metaData
                .getSchemas(
                    connection.getCatalog(),
                    prefix.getDatabaseName() + metaData.getSearchStringEscape()
                )
            ) {
                while (schemas.next()) {
                    final String schemaName = schemas.getString("TABLE_SCHEM").toLowerCase(Locale.ENGLISH);
                    // skip internal schemas
                    if (!schemaName.equals("information_schema")) {
                        names.add(QualifiedName.ofDatabase(name.getCatalogName(), schemaName));
                    }
                }
            }

            // Does user want sorting?
            if (sort != null) {
                // We can only really sort by the database name at this level so ignore SortBy field
                final Comparator<QualifiedName> comparator = Comparator.comparing(QualifiedName::getDatabaseName);
                JdbcConnectorUtils.sort(names, sort, comparator);
            }

            // Does user want pagination?
            final List<QualifiedName> results = JdbcConnectorUtils.paginate(names, pageable);

            log.debug("Finished listing database names for catalog {} for request {}", catalogName, context);
            return results;
        } catch (final SQLException se) {
            throw Throwables.propagate(se);
        }
    }
}
