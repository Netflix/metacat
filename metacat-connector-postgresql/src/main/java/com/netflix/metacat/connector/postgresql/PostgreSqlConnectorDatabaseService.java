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

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorDatabaseService;
import lombok.NonNull;

import javax.annotation.Nonnull;
import jakarta.inject.Inject;
import javax.sql.DataSource;

/**
 * Specific overrides of the JdbcDatabaseService implementation for PostgreSQL.
 *
 * @author tgianos
 * @see JdbcConnectorDatabaseService
 * @since 1.0.0
 */
public class PostgreSqlConnectorDatabaseService extends JdbcConnectorDatabaseService {

    /**
     * Constructor.
     *
     * @param dataSource      The PostgreSQL datasource to use
     * @param exceptionMapper The exception mapper to use to convert from SQLException's to ConnectorException's
     */
    @Inject
    public PostgreSqlConnectorDatabaseService(
        @Nonnull @NonNull final DataSource dataSource,
        @Nonnull @NonNull final JdbcExceptionMapper exceptionMapper
    ) {
        super(dataSource, exceptionMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void create(@Nonnull final ConnectorRequestContext context, @Nonnull final DatabaseInfo resource) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@Nonnull final ConnectorRequestContext context, @Nonnull final QualifiedName name) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }
}
