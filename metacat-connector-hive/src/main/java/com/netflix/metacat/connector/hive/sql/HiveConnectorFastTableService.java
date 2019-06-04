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

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService;
import com.netflix.metacat.connector.hive.HiveConnectorTableService;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.util.HiveTableUtil;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

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
    private IcebergTableHandler icebergTableHandler;

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
     */
    @Autowired
    public HiveConnectorFastTableService(
        final String catalogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final ConnectorContext connectorContext,
        final DirectSqlTable directSqlTable,
        final IcebergTableHandler icebergTableHandler
    ) {
        super(catalogName, metacatHiveClient, hiveConnectorDatabaseService, hiveMetacatConverters, connectorContext);
        this.registry = connectorContext.getRegistry();
        this.directSqlTable = directSqlTable;
        this.icebergTableHandler = icebergTableHandler;

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
        final TableInfo info = super.get(requestContext, name);
        if (!connectorContext.getConfig().isIcebergEnabled() || !HiveTableUtil.isIcebergTable(info)) {
            return info;
        }
        final String tableLoc = HiveTableUtil.getIcebergTableMetadataLocation(info);
        final com.netflix.iceberg.Table icebergTable = this.icebergTableHandler.getIcebergTable(name, tableLoc);
        return this.hiveMetacatConverters.fromIcebergTableToTableInfo(name,
            icebergTable, tableLoc, info.getAudit());
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
        if (HiveTableUtil.isIcebergTable(tableInfo)) {
            requestContext.setIgnoreErrorsAfterUpdate(true);
            try {
                directSqlTable.updateIcebergTable(tableInfo);
            } catch (IllegalStateException e) {
                throw new TablePreconditionFailedException(tableInfo.getName(), e.getMessage());
            }
        } else {
            super.update(requestContext, tableInfo);
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
