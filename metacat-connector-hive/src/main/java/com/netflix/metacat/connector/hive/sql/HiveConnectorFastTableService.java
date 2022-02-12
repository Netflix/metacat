/*
 *  Copyright 2017 Netflix, Inc.
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
 */

package com.netflix.metacat.connector.hive.sql;

import com.google.common.base.Throwables;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService;
import com.netflix.metacat.connector.hive.HiveConnectorTableService;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.commonview.CommonViewHandler;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.metacat.connector.hive.util.HiveTableUtil;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;

import java.util.List;
import java.util.Map;

/**
 * HiveConnectorFastTableService.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public class HiveConnectorFastTableService extends HiveConnectorTableService {
    private final Registry registry;
    private final DirectSqlTable directSqlTable;
    private final IcebergTableHandler icebergTableHandler;
    private final CommonViewHandler commonViewHandler;
    private final HiveConnectorFastTableServiceProxy hiveConnectorFastTableServiceProxy;

    /**
     * Constructor.
     *
     * @param catalogName                  catalog name
     * @param metacatHiveClient            hive client
     * @param hiveConnectorDatabaseService databaseService
     * @param hiveMetacatConverters        hive converter
     * @param connectorContext             serverContext
     * @param directSqlTable               Table jpa service
     * @param icebergTableHandler          iceberg table handler
     * @param commonViewHandler            common view handler
     * @param hiveConnectorFastTableServiceProxy hive connector fast table service proxy
     */
    public HiveConnectorFastTableService(
        final String catalogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final ConnectorContext connectorContext,
        final DirectSqlTable directSqlTable,
        final IcebergTableHandler icebergTableHandler,
        final CommonViewHandler commonViewHandler,
        final HiveConnectorFastTableServiceProxy hiveConnectorFastTableServiceProxy
    ) {
        super(catalogName, metacatHiveClient, hiveConnectorDatabaseService, hiveMetacatConverters, connectorContext);
        this.registry = connectorContext.getRegistry();
        this.directSqlTable = directSqlTable;
        this.icebergTableHandler = icebergTableHandler;
        this.commonViewHandler = commonViewHandler;
        this.hiveConnectorFastTableServiceProxy = hiveConnectorFastTableServiceProxy;
    }

    @Override
    public void create(final ConnectorRequestContext requestContext, final TableInfo tableInfo) {
        try {
            super.create(requestContext, tableInfo);
        } catch (InvalidMetaException e) {
            throw handleException(e);
        }
    }

    private RuntimeException handleException(final RuntimeException e) {
        //
        // On table creation, hive metastore validates the table location.
        // On iceberg table get and update, the iceberg method uses the metadata location.
        // On both occasions, FileSystem uses a relevant file system based on the location scheme. Noticed an error
        // where the s3 client's pool closed abruptly. This causes subsequent request to the s3 client to fail.
        // FileSystem caches the file system instances.
        // The fix is to clear the FileSystem cache so that it can recreate the file system instances.
        //
        for (Throwable ex : Throwables.getCausalChain(e)) {
            if (ex instanceof IllegalStateException && ex.getMessage().contains("Connection pool shut down")) {
                log.warn("File system connection pool is down. It will be restarted.");
                registry.counter(HiveMetrics.CounterHiveFileSystemFailure.getMetricName()).increment();
                try {
                    FileSystem.closeAll();
                } catch (Exception fe) {
                    log.warn("Failed closing the file system.", fe);
                }
                Throwables.propagate(ex);
            }
        }
        throw e;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(final ConnectorRequestContext requestContext, final QualifiedName name) {
        return directSqlTable.exists(name);
    }

    /**
     * getTable.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to get
     * @return table dto
     */
    @Override
    public TableInfo get(final ConnectorRequestContext requestContext, final QualifiedName name) {
        try {
            final TableInfo info = super.get(requestContext, name);
            if (connectorContext.getConfig().isCommonViewEnabled()
                && HiveTableUtil.isCommonView(info)) {
                final String tableLoc = HiveTableUtil.getCommonViewMetadataLocation(info);
                return hiveConnectorFastTableServiceProxy.getCommonViewTableInfo(name, tableLoc, info,
                    new HiveTypeConverter(), connectorContext.getConfig().isIcebergCacheEnabled());
            }
            if (!connectorContext.getConfig().isIcebergEnabled() || !HiveTableUtil.isIcebergTable(info)) {
                return info;
            }
            final String tableLoc = HiveTableUtil.getIcebergTableMetadataLocation(info);
            final TableInfo result = hiveConnectorFastTableServiceProxy.getIcebergTable(name, tableLoc, info,
                requestContext.isIncludeMetadata(), connectorContext.getConfig().isIcebergCacheEnabled());
            // Renamed tables could still be cached with the old table name.
            // Set it to the qName in the request.
            result.setName(name);
            return result;
        } catch (IllegalStateException e) {
            throw handleException(e);
        }
    }


    @Override
    public Map<String, List<QualifiedName>> getTableNames(
        final ConnectorRequestContext context,
        final List<String> uris,
        final boolean prefixSearch
    ) {
        return directSqlTable.getTableNames(uris, prefixSearch);
    }

    /**
     * Update a table with the given metadata.
     *
     * If table is an iceberg table, then lock the table for update so that no other request can update it. If the meta
     * information is invalid, then throw an error.
     * If table is not an iceberg table, then do a regular table update.
     *
     * @param requestContext The request context
     * @param tableInfo      The resource metadata
     */
    @Override
    public void update(final ConnectorRequestContext requestContext, final TableInfo tableInfo) {
        try {
            if (HiveTableUtil.isIcebergTable(tableInfo)) {
                icebergTableHandler.handleUpdate(requestContext, this.directSqlTable, tableInfo);
            } else if (connectorContext.getConfig().isCommonViewEnabled()
                && HiveTableUtil.isCommonView(tableInfo)) {
                commonViewHandler.handleUpdate(requestContext, this.directSqlTable, tableInfo);
            } else {
                super.update(requestContext, tableInfo);
            }
        } catch (IllegalStateException e) {
            throw handleException(e);
        }
    }
    /**
     * {@inheritDoc}.
     */
    @Override
    public void delete(final ConnectorRequestContext requestContext, final QualifiedName name) {
        directSqlTable.delete(name);
    }
}
