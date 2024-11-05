package com.netflix.metacat.connector.hive.client.embedded;

import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.spi.Stoppable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class HiveCustomConnectionProvider implements ConnectionProvider, Stoppable {
    private final DataSource dataSource;

    public HiveCustomConnectionProvider(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void closeConnection(Connection conn) throws SQLException {
        conn.close();
    }

    @Override
    public boolean supportsAggressiveRelease() {
        return false;
    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isUnwrappableAs(Class unwrapType) {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> unwrapType) {
        return null;
    }
}
