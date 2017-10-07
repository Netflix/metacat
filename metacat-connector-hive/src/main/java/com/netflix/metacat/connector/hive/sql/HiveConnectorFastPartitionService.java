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

import com.google.common.base.Strings;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.connector.hive.HiveConnectorPartitionService;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HiveConnectorFastPartitionService.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public class HiveConnectorFastPartitionService extends HiveConnectorPartitionService {
    private DirectSqlGetPartition directSqlGetPartition;
    private DirectSqlSavePartition directSqlSavePartition;

    /**
     * Constructor.
     *
     * @param context               connector context
     * @param metacatHiveClient     hive client
     * @param hiveMetacatConverters hive converter
     * @param directSqlGetPartition service to get partitions
     * @param directSqlSavePartition service to save partitions
     */
    public HiveConnectorFastPartitionService(
        final ConnectorContext context,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final DirectSqlGetPartition directSqlGetPartition,
        final DirectSqlSavePartition directSqlSavePartition
    ) {
        super(context, metacatHiveClient, hiveMetacatConverters);
        this.directSqlGetPartition = directSqlGetPartition;
        this.directSqlSavePartition = directSqlSavePartition;
    }

    /**
     * Number of partitions for the given table.
     *
     * @param tableName tableName
     * @return Number of partitions
     */
    @Override
    public int getPartitionCount(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName
    ) {
        return directSqlGetPartition.getPartitionCount(requestContext, tableName);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<PartitionInfo> getPartitions(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName,
        final PartitionListRequest partitionsRequest
    ) {
        return directSqlGetPartition.getPartitions(requestContext, tableName, partitionsRequest);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionKeys(final ConnectorRequestContext requestContext,
                                         final QualifiedName tableName,
                                         final PartitionListRequest partitionsRequest) {
        return directSqlGetPartition.getPartitionKeys(requestContext, tableName, partitionsRequest);
    }

    /**
     * getPartitionNames.
     *
     * @param uris         uris
     * @param prefixSearch prefixSearch
     * @return partition names
     */
    @Override
    public Map<String, List<QualifiedName>> getPartitionNames(
        @Nonnull final ConnectorRequestContext context,
        @Nonnull final List<String> uris,
        final boolean prefixSearch) {
        return directSqlGetPartition.getPartitionNames(context, uris, prefixSearch);
    }

    @Override
    protected Map<String, PartitionHolder> getPartitionsByNames(final Table table, final List<String> partitionNames) {
        return directSqlGetPartition.getPartitionHoldersByNames(table, partitionNames);
    }

    protected void addUpdateDropPartitions(final QualifiedName tableQName, final Table table,
        final List<String> partitionNames, final List<PartitionInfo> addedPartitionInfos,
        final List<PartitionHolder> existingPartitionHolders, final Set<String> deletePartitionNames) {
        final boolean  useHiveFastServiceForSavePartitions = Boolean.parseBoolean(getContext().getConfiguration()
                .getOrDefault("hive.use.embedded.fastservice.save.partitions", "false"))
            || (table.getParameters() != null && Boolean.parseBoolean(table.getParameters()
                .getOrDefault("hive.use.embedded.fastservice.save.partitions", "false")));
        if (useHiveFastServiceForSavePartitions) {
            if (!existingPartitionHolders.isEmpty()) {
                copyTableSdToPartitionHoldersSd(existingPartitionHolders, table);
            }
            copyTableSdToPartitionInfosSd(addedPartitionInfos, table);
            directSqlSavePartition.addUpdateDropPartitions(tableQName, table, addedPartitionInfos,
                existingPartitionHolders, deletePartitionNames);
        } else {
            super.addUpdateDropPartitions(tableQName, table, partitionNames, addedPartitionInfos,
                existingPartitionHolders, deletePartitionNames);
        }
    }

    private void copyTableSdToPartitionInfosSd(final List<PartitionInfo> partitionInfos, final Table table) {
        //
        // Update the partition info based on that of the table.
        //
        for (PartitionInfo partitionInfo : partitionInfos) {
            copyTableSdToPartitionInfoSd(partitionInfo, table);
        }
    }

    private void copyTableSdToPartitionHoldersSd(final List<PartitionHolder> partitionHolders, final Table table) {
        //
        // Update the partition info based on that of the table.
        //
        for (PartitionHolder partitionHolder : partitionHolders) {
            copyTableSdToPartitionInfoSd(partitionHolder.getPartitionInfo(), table);
        }
    }

    private void copyTableSdToPartitionInfoSd(final PartitionInfo partitionInfo, final Table table) {
        final StorageInfo sd = partitionInfo.getSerde();
        final StorageDescriptor tableSd = table.getSd();

        if (Strings.isNullOrEmpty(sd.getInputFormat())) {
            sd.setInputFormat(tableSd.getInputFormat());
        }
        if (Strings.isNullOrEmpty(sd.getOutputFormat())) {
            sd.setOutputFormat(tableSd.getOutputFormat());
        }
        if (sd.getParameters() == null || sd.getParameters().isEmpty()) {
            sd.setParameters(tableSd.getParameters());
        }
        final SerDeInfo tableSerde = tableSd.getSerdeInfo();
        if (tableSerde != null) {
            if (Strings.isNullOrEmpty(sd.getSerializationLib())) {
                sd.setSerializationLib(tableSerde.getSerializationLib());
            }
            if (sd.getSerdeInfoParameters() == null || sd.getSerdeInfoParameters().isEmpty()) {
                sd.setSerdeInfoParameters(tableSerde.getParameters());
            }
        }
    }
}
