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
package com.netflix.metacat.connector.mysql;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorDatabaseService;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * MySql specific extension of the JdbcConnectorDatabaseService implementation for any differences from default.
 *
 * @author tgianos
 * @since 1.0.0
 */
@Slf4j
public class MySqlConnectorDatabaseService extends JdbcConnectorDatabaseService {

    /**
     * Constructor.
     *
     * @param dataSource      The datasource to use
     * @param exceptionMapper The exception mapper to use
     */
    @Inject
    public MySqlConnectorDatabaseService(
        @Nonnull @NonNull final DataSource dataSource,
        @Nonnull @NonNull final JdbcExceptionMapper exceptionMapper
    ) {
        super(dataSource, exceptionMapper);
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
        // Overrides the super class due to MySQL using catalog instead of schemas when trying to list database names
        final String catalogName = name.getCatalogName();
        log.debug("Beginning to list database names for catalog {} for request {}", catalogName, context);

        try (
            final Connection connection = this.getDataSource().getConnection();
            final ResultSet schemas = connection.getMetaData().getCatalogs()
        ) {
            final List<QualifiedName> names = Lists.newArrayList();
            while (schemas.next()) {
                final String schemaName = schemas.getString("TABLE_CAT").toLowerCase(Locale.ENGLISH);
                // skip internal schemas
                if (!schemaName.equals("information_schema") && !schemaName.equals("mysql")) {
                    if (prefix == null) {
                        names.add(QualifiedName.ofDatabase(name.getCatalogName(), schemaName));
                    } else if (StringUtils.isNotBlank(prefix.getDatabaseName())
                        && schemaName.startsWith(prefix.getDatabaseName())) {
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
            throw this.getExceptionMapper().toConnectorException(se, name);
        }
    }
}
