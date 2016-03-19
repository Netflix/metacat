package com.netflix.metacat.plugin.postgresql;

import com.facebook.presto.plugin.ColumnDetailHandle;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.Map;

import static com.facebook.presto.plugin.jdbc.Types.checkType;

/**
 * Created by amajumdar on 9/30/15.
 */
public class PostgreSqlJdbcMetadata extends JdbcMetadata{
    private final MetacatPostgreSqlClient jdbcClient;
    @Inject
    public PostgreSqlJdbcMetadata(JdbcConnectorId connectorId,
            JdbcClient jdbcClient,
            JdbcMetadataConfig config) {
        super(connectorId, jdbcClient, config);
        this.jdbcClient = (MetacatPostgreSqlClient) jdbcClient;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = checkType(table, JdbcTableHandle.class, "tableHandle");

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (ColumnDetailHandle column : jdbcClient.getColumnsWithDetails(handle)) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle jdbcTableHandle = checkType(tableHandle, JdbcTableHandle.class, "tableHandle");

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnDetailHandle column : jdbcClient.getColumnsWithDetails(jdbcTableHandle)) {
            columnHandles.put(column.getColumnMetadata().getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, JdbcTableHandle.class, "tableHandle");
        return checkType(columnHandle, ColumnDetailHandle.class, "columnHandle").getColumnMetadata();
    }
}
