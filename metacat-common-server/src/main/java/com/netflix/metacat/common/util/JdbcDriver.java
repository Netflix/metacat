/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.common.util;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Jdbc driver wrapper.
 */
public class JdbcDriver implements Driver {
    private DataSource datasource;
    private Driver driver;

    /**
     * Constructor.
     * @param driver driver
     * @param datasource data source
     */
    public JdbcDriver(final Driver driver, final DataSource datasource) {
        this.driver = driver;
        this.datasource = datasource;
    }

    @Override
    public Connection connect(final String url, final Properties info) throws SQLException {
        return datasource.getConnection();
    }

    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        return driver.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
        return driver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return driver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return driver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return driver.getParentLogger();
    }
}
