package com.netflix.metacat.connector.mysql;

import com.google.inject.Inject;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorTableService;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Mysql table service implementation.
 *
 * @author amajumdar
 * @since 1.2.0
 */
@Slf4j
public class MySqlConnectorTableService extends JdbcConnectorTableService {
    private static final String COL_CREATE_TIME = "create_time";
    private static final String COL_UPDATE_TIME = "update_time";
    private static final String SQL_GET_AUDIT_INFO
        = "select create_time, update_time from information_schema.tables where table_schema=? and table_name=?";
    /**
     * Constructor.
     *
     * @param dataSource      the datasource to use to connect to the database
     * @param typeConverter   The type converter to use from the SQL type to Metacat canonical type
     * @param exceptionMapper The exception mapper to use
     */
    @Inject
    public MySqlConnectorTableService(
        final DataSource dataSource,
        final JdbcTypeConverter typeConverter,
        final JdbcExceptionMapper exceptionMapper) {
        super(dataSource, typeConverter, exceptionMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setTableInfoDetails(final Connection connection, final TableInfo tableInfo) {
        final QualifiedName tableName = tableInfo.getName();
        try (
            final PreparedStatement statement = connection.prepareStatement(SQL_GET_AUDIT_INFO)
        ) {
            statement.setString(1, tableName.getDatabaseName());
            statement.setString(2, tableName.getTableName());
            try (final ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    final AuditInfo auditInfo =
                        AuditInfo.builder().createdDate(resultSet.getDate(COL_CREATE_TIME))
                            .lastModifiedDate(resultSet.getDate(COL_UPDATE_TIME)).build();
                    tableInfo.setAudit(auditInfo);
                }
            }
        } catch (final Exception ignored) {
            log.info("Ignoring. Error getting the audit info for table {}", tableName);
        }
    }
}
