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
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Utility methods for working with JDBC connections.
 *
 * @author tgianos
 * @since 1.0.0
 */
public final class JdbcConnectorUtils extends ConnectorUtils {

    /**
     * Utility class constructor private.
     */
    protected JdbcConnectorUtils() {
    }

    /**
     * Execute a SQL update statement against the given datasource.
     *
     * @param connection The connection to attempt to execute an update against
     * @param sql        The sql to execute
     * @return The number of rows updated or exception
     */
    static int executeUpdate(@Nonnull @NonNull final Connection connection, @Nonnull @NonNull final String sql) {
        try (final Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        } catch (final SQLException se) {
            throw Throwables.propagate(se);
        }
    }
}
