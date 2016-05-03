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

package com.netflix.metacat.plugin.mysql;

import com.facebook.presto.plugin.ColumnDetailHandle;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.mysql.MySqlConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.FloatType;
import com.facebook.presto.type.IntType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.mysql.jdbc.Driver;
import com.netflix.metacat.common.util.DataSourceManager;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.type.FloatType.FLOAT;
import static com.facebook.presto.type.IntType.INT;
import static java.util.Locale.ENGLISH;

public class MetacatMySqlClient extends BaseJdbcClient {
    private static final Map<Type, String> METACAT_SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(INT, IntType.TYPE)
            .put(FLOAT, FloatType.TYPE)
            .build();
    @Inject
    public MetacatMySqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config, MySqlConfig mySqlConfig) throws SQLException {
        super(connectorId, config, "`", DataSourceManager.get().getDriver(connectorId.toString(), new Driver()));
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        if (mySqlConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(mySqlConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(mySqlConfig.getMaxReconnects()));
        }
        if (mySqlConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(mySqlConfig.getConnectionTimeout().toMillis()));
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName) throws SQLException {
        // For Metacat's purposes a view and a table are the same
        return connection.getMetaData().getTables(schemaName, null, tableName, new String[] { "TABLE", "VIEW" });
    }

    @Override
    protected Type toPrestoType(int jdbcType)
    {
        switch (jdbcType) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
            return INT;
        case Types.FLOAT:
        case Types.REAL:
            return FLOAT;
        default:
            return super.toPrestoType(jdbcType);
        }
    }

    protected String toSqlType(Type type)
    {
        String sqlType = METACAT_SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        } else {
            sqlType = super.toSqlType(type);
            switch (sqlType) {
            case "varchar":
                return "mediumtext";
            case "varbinary":
                return "mediumblob";
            case "time with timezone":
                return "time";
            case "timestamp":
            case "timestamp with timezone":
                return "datetime";
            }
            return sqlType;
        }
    }


    public List<ColumnDetailHandle> getColumnsWithDetails(JdbcTableHandle tableHandle)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet resultSet = metadata.getColumns(tableHandle.getCatalogName(), tableHandle.getSchemaName(),
                    tableHandle.getTableName(), null);
                 ResultSet indexSet = metadata.getIndexInfo(tableHandle.getCatalogName(), tableHandle.getSchemaName(),
                         tableHandle.getTableName(), false, true)) {
                List<ColumnDetailHandle> columns = new ArrayList<>();
                Set<String> indexColumns = Sets.newHashSet();
                while( indexSet.next()){
                    String columnName = indexSet.getString("COLUMN_NAME");
                    if( columnName != null) {
                        indexColumns.add(columnName);
                    }
                }
                boolean found = false;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet.getInt("DATA_TYPE"));

                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        String sourceType = resultSet.getString("TYPE_NAME");
                        Integer size = resultSet.getInt("COLUMN_SIZE");
                        Boolean isNullable = "yes".equalsIgnoreCase(resultSet.getString("IS_NULLABLE"));
                        String defaultValue = resultSet.getString("COLUMN_DEF");
                        String comment = resultSet.getString("REMARKS");
                        Boolean isIndexKey = indexColumns.contains(columnName);
                        columns.add(new ColumnDetailHandle(connectorId, columnName, columnType, false, comment, sourceType, size, isNullable, defaultValue, null, isIndexKey));
                    }
                }
                if (!found) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED, "Table has no supported column types: " + tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Set<String> getSchemaNames()
    {
        // for MySQL, we need to list catalogs instead of schemas
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
                ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH);
                // skip internal schemas
                if (!schemaName.equals("information_schema") && !schemaName.equals("mysql")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        // MySQL uses catalogs instead of schemas
        return new SchemaTableName(
                resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));

    }
}
