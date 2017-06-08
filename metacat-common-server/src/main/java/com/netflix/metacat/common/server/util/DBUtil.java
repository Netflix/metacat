/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.common.server.util;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * DB utility.
 */
//TODO change to spring jdbc template
@Slf4j
public final class DBUtil {
    private DBUtil() {
    }

    /**
     * Returns a read only connection.
     * @param dataSource data source
     * @return connection
     */
    public static Connection getReadConnection(final DataSource dataSource) {
        Connection result = null;
        try {
            result = dataSource.getConnection();
            result.setAutoCommit(true);
            result.setReadOnly(true);
        } catch (SQLException e) {
            closeReadConnection(result);
            log.error("Sql exception", e);
            throw new RuntimeException("Failed to get connection", e);
        }
        return result;
    }

    /**
     * Closes the connection.
     * @param conn connection
     */
    public static void closeReadConnection(@Nullable final Connection conn) {
        if (conn != null) {
            try {
                conn.setAutoCommit(false);
                conn.setReadOnly(false);
                conn.close();
            } catch (SQLException e) {
                log.error("Sql exception", e);
                throw new RuntimeException("Failed to close connection", e);
            }
        }
    }
}
