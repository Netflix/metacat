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
package com.netflix.metacat.connector.postgresql;

import com.google.inject.Inject;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorTableService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * PostgreSQL table service implementation.
 *
 * @author tgianos
 * @since 1.0.0
 */
@Slf4j
public class PostgreSqlConnectorTableService extends JdbcConnectorTableService {

    /**
     * Constructor.
     *
     * @param dataSource      the datasource to use to connect to the database
     * @param typeConverter   The type converter to use from the SQL type to Metacat canonical type
     * @param exceptionMapper The exception mapper to use
     */
    @Inject
    public PostgreSqlConnectorTableService(
        @Nonnull @NonNull final DataSource dataSource,
        @Nonnull @NonNull final JdbcTypeConverter typeConverter,
        @Nonnull @NonNull final JdbcExceptionMapper exceptionMapper
    ) {
        super(dataSource, typeConverter, exceptionMapper);
    }

    /**
     * {@inheritDoc}
     * <p>
     * A Metacat database is a PostgreSQL schema, not a PostgreSQL database, so metadata lookups
     * have to be scoped to the catalog the connection is already attached to. Drivers before
     * 42.7 ignored the catalog argument; from 42.7 on they filter by it and passing the schema
     * name here matches nothing.
     */
    @Override
    protected String getJdbcCatalog(
        @Nonnull @NonNull final Connection connection,
        @Nonnull @NonNull final String database
    ) throws SQLException {
        return connection.getCatalog();
    }
}
