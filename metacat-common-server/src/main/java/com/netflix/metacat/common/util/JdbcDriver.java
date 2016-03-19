package com.netflix.metacat.common.util;

import com.google.common.base.Throwables;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by amajumdar on 3/15/16.
 */
public class JdbcDriver implements Driver {
    private DataSource datasource;
    private Driver driver;

    public JdbcDriver(Driver driver, DataSource datasource) {
        this.driver = driver;
        this.datasource = datasource;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return datasource.getConnection();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return driver.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return driver.getPropertyInfo( url, info);
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
