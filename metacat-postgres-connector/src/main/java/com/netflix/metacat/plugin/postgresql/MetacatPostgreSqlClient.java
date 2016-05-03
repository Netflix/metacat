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

package com.netflix.metacat.plugin.postgresql;

import com.facebook.presto.plugin.ColumnDetailHandle;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.FloatType;
import com.facebook.presto.type.IntType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.util.DataSourceManager;
import io.airlift.slice.Slice;
import org.postgresql.Driver;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.type.FloatType.FLOAT;
import static com.facebook.presto.type.IntType.INT;


public class MetacatPostgreSqlClient extends BaseJdbcClient {
    private static final String SQL_GET_DIST_SORT_KEYS = "select t.column, t.distkey, t.sortkey from pg_catalog.pg_table_def t where t.schemaname=? and t.tablename=?";
    private static final Map<Type, String> METACAT_SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(INT, IntType.TYPE)
            .put(FLOAT, FloatType.TYPE)
            .build();
    @Inject
    public MetacatPostgreSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config) throws SQLException {
        super(connectorId, config, "\"", DataSourceManager.get().getDriver(connectorId.toString(), new Driver()));
    }

    public List<ColumnDetailHandle> getColumnsWithDetails(JdbcTableHandle tableHandle)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet resultSet = metadata.getColumns(tableHandle.getCatalogName(), tableHandle.getSchemaName(),
                    tableHandle.getTableName(), null)) {
                List<ColumnDetailHandle> columns = new ArrayList<>();
                Map<String, Map.Entry<Boolean, Boolean>> distSortMap = getDistSortMap(tableHandle.getSchemaName(),
                        tableHandle.getTableName());
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
                        Map.Entry<Boolean, Boolean> distSort = distSortMap.get(columnName);
                        Boolean isPartitionKey = Boolean.FALSE;
                        Boolean isSortKey = Boolean.FALSE;
                        if( distSort != null){
                            isPartitionKey = distSort.getKey();
                            isSortKey = distSort.getValue();
                        }
                        columns.add(new ColumnDetailHandle(connectorId, columnName, columnType, isPartitionKey, comment, sourceType, size, isNullable, defaultValue, isSortKey, null));
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

    @SuppressWarnings("JpaQueryApiInspection")
    private Map<String, Map.Entry<Boolean, Boolean>> getDistSortMap(String schemaName, String tableName){
        Map<String, Map.Entry<Boolean, Boolean>> result = Maps.newHashMap();
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
                PreparedStatement stmt = connection.prepareStatement(SQL_GET_DIST_SORT_KEYS)) {
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            try(ResultSet resultSet = stmt.executeQuery()){
                while( resultSet.next()){
                    String columnName = resultSet.getString("column");
                    Boolean distKey = resultSet.getBoolean("distkey");
                    Boolean sortKey = resultSet.getBoolean("sortkey");
                    result.put( columnName, new AbstractMap.SimpleImmutableEntry<>(distKey, sortKey));
                }
            }
        }catch (SQLException ignored) {}
        return result;
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle, Collection<Slice> fragments)
    {
        // PostgreSQL does not allow qualifying the target of a rename
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" RENAME TO ")
                .append(quoted(handle.getTableName()));

        try (Connection connection = getConnection(handle)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
