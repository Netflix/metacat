/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package com.netflix.metacat.connector.snowflake;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorTableService;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Snowflake table service implementation.
 *
 * @author amajumdar
 * @since 1.2.0
 */
@Slf4j
public class SnowflakeConnectorTableService extends JdbcConnectorTableService {
    private static final String COL_CREATED = "CREATED";
    private static final String COL_LAST_ALTERED = "LAST_ALTERED";
    private static final String SQL_GET_AUDIT_INFO
        = "select created, last_altered from information_schema.tables"
        + " where table_schema=? and table_name=?";
    private static final String JDBC_UNDERSCORE = "_";
    private static final String JDBC_ESCAPE_UNDERSCORE = "\\_";

    /**
     * Constructor.
     *
     * @param dataSource      the datasource to use to connect to the database
     * @param typeConverter   The type converter to use from the SQL type to Metacat canonical type
     * @param exceptionMapper The exception mapper to use
     */
    @Inject
    public SnowflakeConnectorTableService(
        final DataSource dataSource,
        final JdbcTypeConverter typeConverter,
        final JdbcExceptionMapper exceptionMapper
    ) {
        super(dataSource, typeConverter, exceptionMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(@Nonnull final ConnectorRequestContext context, @Nonnull final QualifiedName name) {
        super.delete(context, getSnowflakeName(name));
    }

    /**
     * Returns the snowflake represented name which is always uppercase.
     *
     * @param name qualified name
     * @return qualified name
     */
    private QualifiedName getSnowflakeName(final QualifiedName name) {
        return name.cloneWithUpperCase();
    }

    /**
     * Returns a normalized string that escapes JDBC special characters like "_".
     *
     * @param input object name.
     * @return the normalized string.
     */
    private static String getJdbcNormalizedSnowflakeName(final String input) {
        if (!StringUtils.isBlank(input) && input.contains(JDBC_UNDERSCORE)) {
            return StringUtils.replace(input, JDBC_UNDERSCORE, JDBC_ESCAPE_UNDERSCORE);
        }
        return input;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableInfo get(@Nonnull final ConnectorRequestContext context, @Nonnull final QualifiedName name) {
        return super.get(context, getSnowflakeName(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<TableInfo> list(@Nonnull final ConnectorRequestContext context,
                                @Nonnull final QualifiedName name,
                                @Nullable final QualifiedName prefix,
                                @Nullable final Sort sort,
                                @Nullable final Pageable pageable) {
        return super.list(context, getSnowflakeName(name), prefix, sort, pageable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> listNames(@Nonnull final ConnectorRequestContext context,
                                         @Nonnull final QualifiedName name,
                                         @Nullable final QualifiedName prefix,
                                         @Nullable final Sort sort,
                                         @Nullable final Pageable pageable) {
        return super.listNames(context, name.cloneWithUpperCase(), prefix, sort, pageable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rename(@Nonnull final ConnectorRequestContext context,
                       @Nonnull final QualifiedName oldName,
                       @Nonnull final QualifiedName newName) {
        super.rename(context, getSnowflakeName(oldName), getSnowflakeName(newName));
    }

    @Override
    protected Connection getConnection(@Nonnull @NonNull final String schema) throws SQLException {
        final Connection connection = this.dataSource.getConnection();
        connection.setSchema(connection.getCatalog());
        return connection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(@Nonnull final ConnectorRequestContext context, @Nonnull final QualifiedName name) {
        boolean result = false;
        final QualifiedName sName = getSnowflakeName(name);
        try (Connection connection = this.dataSource.getConnection()) {
            final ResultSet rs = getTables(connection, sName, sName, false);
            if (rs.next()) {
                result = true;
            }
        } catch (final SQLException se) {
            throw this.exceptionMapper.toConnectorException(se, name);
        }
        return result;
    }

    @Override
    protected ResultSet getColumns(
        @Nonnull @NonNull final Connection connection,
        @Nonnull @NonNull final QualifiedName name
    ) throws SQLException {
        try {
            return connection.getMetaData().getColumns(
                    connection.getCatalog(),
                    getJdbcNormalizedSnowflakeName(name.getDatabaseName()),
                    getJdbcNormalizedSnowflakeName(name.getTableName()),
                    JdbcConnectorUtils.MULTI_CHARACTER_SEARCH
            );
        } catch (SQLException e) {
            throw this.exceptionMapper.toConnectorException(e, name);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setTableInfoDetails(final Connection connection, final TableInfo tableInfo) {
        final QualifiedName tableName = getSnowflakeName(tableInfo.getName());
        try (
            PreparedStatement statement = connection.prepareStatement(SQL_GET_AUDIT_INFO)
        ) {
            statement.setString(1, tableName.getDatabaseName());
            statement.setString(2, tableName.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    final AuditInfo auditInfo =
                        AuditInfo.builder().createdDate(resultSet.getDate(COL_CREATED))
                            .lastModifiedDate(resultSet.getDate(COL_LAST_ALTERED)).build();
                    tableInfo.setAudit(auditInfo);
                }
            }
        } catch (final Exception ignored) {
            log.info("Ignoring. Error getting the audit info for table {}", tableName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ResultSet getTables(
        @Nonnull @NonNull final Connection connection,
        @Nonnull @NonNull final QualifiedName name,
        @Nullable final QualifiedName prefix
    ) throws SQLException {
        return getTables(connection, name, prefix, true);
    }

    private ResultSet getTables(
        @Nonnull @NonNull final Connection connection,
        @Nonnull @NonNull final QualifiedName name,
        @Nullable final QualifiedName prefix,
        final boolean multiCharacterSearch
    ) throws SQLException {
        final String schema = getJdbcNormalizedSnowflakeName(name.getDatabaseName());
        final DatabaseMetaData metaData = connection.getMetaData();
        return prefix == null || StringUtils.isEmpty(prefix.getTableName())
            ? metaData.getTables(connection.getCatalog(), schema, null, TABLE_TYPES)
            : metaData
            .getTables(
                connection.getCatalog(),
                schema,
                multiCharacterSearch ? getJdbcNormalizedSnowflakeName(prefix.getTableName())
                        + JdbcConnectorUtils.MULTI_CHARACTER_SEARCH
                    : getJdbcNormalizedSnowflakeName(prefix.getTableName()),
                TABLE_TYPES
            );
    }
}
