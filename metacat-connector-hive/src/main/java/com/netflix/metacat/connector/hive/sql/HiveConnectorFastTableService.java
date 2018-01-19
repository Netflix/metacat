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
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    /**
     * Constructor.
     *
     * @param catalogName                  catalog name
     * @param metacatHiveClient            hive client
     * @param hiveConnectorDatabaseService databaseService
     * @param hiveMetacatConverters        hive converter
     * @param connectorContext             serverContext
     * @param directSqlTable               Table jpa service
     */
    @Autowired
    public HiveConnectorFastTableService(
        final String catalogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final ConnectorContext connectorContext,
        final DirectSqlTable directSqlTable
    ) {
        super(catalogName, metacatHiveClient, hiveConnectorDatabaseService, hiveMetacatConverters, connectorContext);
        this.registry = connectorContext.getRegistry();
        this.directSqlTable = directSqlTable;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(final ConnectorRequestContext requestContext, final QualifiedName name) {
        return directSqlTable.exists(name);
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
        if (isIcebergTable(tableInfo)) {
            final QualifiedName tableName = tableInfo.getName();
            final Long tableId = directSqlTable.getTableId(tableName);
            try {
                log.debug("Locking Iceberg table {}", tableName);
                directSqlTable.lockIcebergTable(tableId, tableName);
                try {
                    final TableInfo existingTableInfo = get(requestContext, tableInfo.getName());
                    if (!isValidIcebergUpdate(existingTableInfo, tableInfo)) {
                        throw new IllegalStateException("Invalid iceberg table metadata. "
                            + "Existing metadata location does not match the given previous metadata location");
                    } else {
                        final Table existingTable = getHiveMetacatConverters().fromTableInfo(existingTableInfo);
                        super.update(requestContext, existingTable, tableInfo);
                    }
                } finally {
                    directSqlTable.unlockIcebergTable(tableId);
                    log.debug("Unlocked Iceberg table {}", tableName);
                }
            } catch (IllegalStateException e) {
                throw new TablePreconditionFailedException(tableName, e.getMessage());
            }
        } else {
            super.update(requestContext, tableInfo);
        }
    }

    private boolean isValidIcebergUpdate(final TableInfo existingTableInfo, final TableInfo newTableInfo) {
        final Map<String, String> existingMetadata = existingTableInfo.getMetadata();
        final Map<String, String> newMetadata = newTableInfo.getMetadata();
        final String existingMetadataLocation = existingMetadata != null
            ? existingMetadata.get(DirectSqlTable.PARAM_METADATA_LOCATION) : null;
        final String previousMetadataLocation = newMetadata != null
            ? newMetadata.get(DirectSqlTable.PARAM_PREVIOUS_METADATA_LOCATION) : null;
        if (StringUtils.isNotBlank(existingMetadataLocation)
            && Objects.equals(existingMetadataLocation, previousMetadataLocation)) {
            return true;
        } else {
            log.info("Invalid iceberg table metadata location (expected:{}, given:{})",
                existingMetadataLocation, previousMetadataLocation);
            return false;
        }
    }

    private boolean isIcebergTable(final TableInfo tableInfo) {
        return tableInfo.getMetadata() != null
            && tableInfo.getMetadata().containsKey(DirectSqlTable.PARAM_TABLE_TYPE)
            && DirectSqlTable.ICEBERG_TABLE_TYPE.equals(tableInfo.getMetadata().get(DirectSqlTable.PARAM_TABLE_TYPE));
    }
}
