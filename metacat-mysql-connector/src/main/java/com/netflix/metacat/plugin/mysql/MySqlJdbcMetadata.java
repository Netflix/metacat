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
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.Types;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.Map;

/**
 * Connector metadata.
 */
public class MySqlJdbcMetadata extends JdbcMetadata {
    private final MetacatMySqlClient jdbcClient;

    /**
     * Constructor.
     * @param connectorId connector id
     * @param jdbcClient jdbc client
     * @param config config
     */
    @Inject
    public MySqlJdbcMetadata(final JdbcConnectorId connectorId,
        final JdbcClient jdbcClient,
        final JdbcMetadataConfig config) {
        super(connectorId, jdbcClient, config);
        this.jdbcClient = (MetacatMySqlClient) jdbcClient;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(final ConnectorSession session, final ConnectorTableHandle table) {
        final JdbcTableHandle handle = Types.checkType(table, JdbcTableHandle.class, "tableHandle");

        final ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (ColumnDetailHandle column : jdbcClient.getColumnsWithDetails(handle)) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(final ConnectorSession session,
        final ConnectorTableHandle tableHandle) {
        final JdbcTableHandle jdbcTableHandle = Types.checkType(tableHandle, JdbcTableHandle.class, "tableHandle");

        final ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnDetailHandle column : jdbcClient.getColumnsWithDetails(jdbcTableHandle)) {
            columnHandles.put(column.getColumnMetadata().getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(final ConnectorSession session, final ConnectorTableHandle tableHandle,
        final ColumnHandle columnHandle) {
        Types.checkType(tableHandle, JdbcTableHandle.class, "tableHandle");
        return Types.checkType(columnHandle, ColumnDetailHandle.class, "columnHandle").getColumnMetadata();
    }
}
