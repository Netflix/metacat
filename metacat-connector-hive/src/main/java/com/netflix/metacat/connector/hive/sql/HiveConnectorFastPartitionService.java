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

import com.google.common.annotations.VisibleForTesting;
import com.netflix.iceberg.ScanSummary;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.HiveConnectorPartitionService;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableUtil;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import com.netflix.metacat.connector.hive.util.HiveTableUtil;
import com.netflix.metacat.connector.hive.util.PartitionUtil;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    private Warehouse warehouse;
    private Registry registry;
    @VisibleForTesting
    private IcebergTableUtil icebergTableUtil;

    /**
     * Constructor.
     *
     * @param context                connector context
     * @param metacatHiveClient      hive client
     * @param warehouse              hive warehouse
     * @param hiveMetacatConverters  hive converter
     * @param directSqlGetPartition  service to get partitions
     * @param directSqlSavePartition service to save partitions
     */
    public HiveConnectorFastPartitionService(
        final ConnectorContext context,
        final IMetacatHiveClient metacatHiveClient,
        final Warehouse warehouse,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final DirectSqlGetPartition directSqlGetPartition,
        final DirectSqlSavePartition directSqlSavePartition
    ) {
        super(context, metacatHiveClient, hiveMetacatConverters);
        this.warehouse = warehouse;
        this.directSqlGetPartition = directSqlGetPartition;
        this.directSqlSavePartition = directSqlSavePartition;
        this.registry = context.getRegistry();
        this.icebergTableUtil = new IcebergTableUtil(context);
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
        final QualifiedName tableName,
        final TableInfo tableInfo
    ) {
        if (HiveTableUtil.isIcebergTable(tableInfo)) {
            throw new MetacatNotSupportedException("IcebergTable Unsupported Operation!");
        }
        return directSqlGetPartition.getPartitionCount(requestContext, tableName);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<PartitionInfo> getPartitions(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName,
        final PartitionListRequest partitionsRequest,
        final TableInfo tableInfo) {
        return (HiveTableUtil.isIcebergTable(tableInfo))
            ? getIcebergPartitionInfos(tableInfo, partitionsRequest)
            : directSqlGetPartition.getPartitions(requestContext, tableName, partitionsRequest);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionKeys(final ConnectorRequestContext requestContext,
                                         final QualifiedName tableName,
                                         final PartitionListRequest partitionsRequest,
                                         final TableInfo tableInfo) {

        return (HiveTableUtil.isIcebergTable(tableInfo))
            ? getIcebergPartitionInfos(tableInfo, partitionsRequest)
            .stream().map(info -> info.getName().getPartitionName()).collect(Collectors.toList())
            :
            directSqlGetPartition.getPartitionKeys(requestContext, tableName, partitionsRequest);

    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionUris(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName,
        final PartitionListRequest partitionsRequest,
        final TableInfo tableInfo
    ) {
        if (HiveTableUtil.isIcebergTable(tableInfo)) {
            throw new MetacatNotSupportedException("IcebergTable Unsupported Operation!");
        }
        return directSqlGetPartition.getPartitionUris(requestContext, tableName, partitionsRequest);
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
        //This is internal call, always turn off the auditTable processing
        return directSqlGetPartition.getPartitionHoldersByNames(table, partitionNames, true);
    }

    protected void addUpdateDropPartitions(final QualifiedName tableQName,
                                           final Table table,
                                           final List<String> partitionNames,
                                           final List<PartitionInfo> addedPartitionInfos,
                                           final List<PartitionHolder> existingPartitionHolders,
                                           final Set<String> deletePartitionNames) {
        final boolean useHiveFastServiceForSavePartitions = Boolean.parseBoolean(getContext().getConfiguration()
            .getOrDefault("hive.use.embedded.sql.save.partitions", "false"))
            || (table.getParameters() != null && Boolean.parseBoolean(table.getParameters()
            .getOrDefault("hive.use.embedded.sql.save.partitions", "false")));
        if (useHiveFastServiceForSavePartitions) {
            final long start = registry.clock().wallTime();
            try {
                if (!existingPartitionHolders.isEmpty()) {
                    final List<PartitionInfo> existingPartitionInfos = existingPartitionHolders.stream()
                        .map(PartitionHolder::getPartitionInfo).collect(Collectors.toList());
                    copyTableSdToPartitionInfosSd(existingPartitionInfos, table);
                    createLocationForPartitions(tableQName, existingPartitionInfos, table);
                }
                copyTableSdToPartitionInfosSd(addedPartitionInfos, table);
                createLocationForPartitions(tableQName, addedPartitionInfos, table);
            } finally {
                registry.timer(registry
                    .createId(HiveMetrics.TagCreatePartitionLocations.getMetricName()).withTags(tableQName.parts()))
                    .record(registry.clock().wallTime() - start, TimeUnit.MILLISECONDS);
            }
            directSqlSavePartition.addUpdateDropPartitions(tableQName, table, addedPartitionInfos,
                existingPartitionHolders, deletePartitionNames);
        } else {
            super.addUpdateDropPartitions(tableQName, table, partitionNames, addedPartitionInfos,
                existingPartitionHolders, deletePartitionNames);
        }
    }

    private void createLocationForPartitions(final QualifiedName tableQName,
                                             final List<PartitionInfo> partitionInfos, final Table table) {
        final boolean doFileSystemCalls = Boolean.parseBoolean(getContext().getConfiguration()
            .getOrDefault("hive.metastore.use.fs.calls", "true"))
            || (table.getParameters() != null && Boolean.parseBoolean(table.getParameters()
            .getOrDefault("hive.metastore.use.fs.calls", "false")));
        partitionInfos.forEach(partitionInfo ->
            createLocationForPartition(tableQName, partitionInfo, table, doFileSystemCalls));
    }

    private void createLocationForPartition(final QualifiedName tableQName,
                                            final PartitionInfo partitionInfo,
                                            final Table table,
                                            final boolean doFileSystemCalls) {
        String location = partitionInfo.getSerde().getUri();
        Path path = null;
        if (StringUtils.isBlank(location)) {
            if (table.getSd() == null || table.getSd().getLocation() == null) {
                throw new InvalidMetaException(tableQName, null);
            }
            final String partitionName = partitionInfo.getName().getPartitionName();
            final List<String> partValues = PartitionUtil
                .getPartValuesFromPartName(tableQName, table, partitionName);
            final String escapedPartName = PartitionUtil.makePartName(table.getPartitionKeys(), partValues);
            path = new Path(table.getSd().getLocation(), escapedPartName);
        } else {
            try {
                path = warehouse.getDnsPath(new Path(location));
            } catch (Exception e) {
                throw new InvalidMetaException(String.format("Failed forming partition location; %s", location), e);
            }
        }
        if (path != null) {
            location = path.toString();
            partitionInfo.getSerde().setUri(location);
            if (doFileSystemCalls) {
                registry.counter(registry.createId(HiveMetrics.CounterHivePartitionFileSystemCall.getMetricName())
                    .withTags(tableQName.parts())).increment();
                try {
                    if (!warehouse.isDir(path)) {
                        //
                        // Added to track the number of partition locations that do not exist before
                        // adding the partition metadata
                        registry.counter(registry.createId(HiveMetrics.CounterHivePartitionPathIsNotDir.getMetricName())
                            .withTags(tableQName.parts())).increment();
                        log.info(String.format("Partition location %s does not exist for table %s",
                            location, tableQName));
                        if (!warehouse.mkdirs(path, false)) {
                            throw new InvalidMetaException(String
                                .format("%s is not a directory or unable to create one", location), null);
                        }
                    }
                } catch (Exception e) {
                    throw new InvalidMetaException(String.format("Failed creating partition location; %s", location),
                        e);
                }
            }
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

    private void copyTableSdToPartitionInfoSd(final PartitionInfo partitionInfo, final Table table) {
        StorageInfo sd = partitionInfo.getSerde();
        //
        // Partitions can be provided in the request without the storage information.
        //
        if (sd == null) {
            sd = new StorageInfo();
            partitionInfo.setSerde(sd);
        }
        final StorageDescriptor tableSd = table.getSd();

        if (StringUtils.isBlank(sd.getInputFormat())) {
            sd.setInputFormat(tableSd.getInputFormat());
        }
        if (StringUtils.isBlank(sd.getOutputFormat())) {
            sd.setOutputFormat(tableSd.getOutputFormat());
        }
        if (sd.getParameters() == null || sd.getParameters().isEmpty()) {
            sd.setParameters(tableSd.getParameters());
        }
        final SerDeInfo tableSerde = tableSd.getSerdeInfo();
        if (tableSerde != null) {
            if (StringUtils.isBlank(sd.getSerializationLib())) {
                sd.setSerializationLib(tableSerde.getSerializationLib());
            }
            if (sd.getSerdeInfoParameters() == null || sd.getSerdeInfoParameters().isEmpty()) {
                sd.setSerdeInfoParameters(tableSerde.getParameters());
            }
        }
    }


    /**
     * {@inheritDoc}.
     */
    @Override
    public void deletePartitions(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName,
        final List<String> partitionNames,
        final TableInfo tableInfo
    ) {
        //TODO: implemented as next step
        if (HiveTableUtil.isIcebergTable(tableInfo)) {
            throw new MetacatNotSupportedException("IcebergTable Unsupported Operation!");
        }
        //The direct sql based deletion doesn't check if the partition is valid
        if (Boolean.parseBoolean(getContext().getConfiguration()
            .getOrDefault(HiveConfigConstants.USE_FAST_DELETION, "false"))) {
            directSqlSavePartition.delete(tableName, partitionNames);
        } else {
            //will throw exception if the partitions are invalid
            super.deletePartitions(requestContext, tableName, partitionNames, tableInfo);
        }
    }

    /**
     * get iceberg table partition summary.
     *
     * @param tableInfo         table info
     * @param partitionsRequest partition request
     * @return iceberg partition name and metrics mapping
     */
    private List<PartitionInfo> getIcebergPartitionInfos(
        final TableInfo tableInfo,
        final PartitionListRequest partitionsRequest) {
        final QualifiedName tableName = tableInfo.getName();
        final com.netflix.iceberg.Table icebergTable = this.icebergTableUtil.getIcebergTable(tableName,
            HiveTableUtil.getIcebergTableMetadataLocation(tableInfo));
        //TODO: to support filter
        //final String filter = partitionsRequest.getFilter();
        final Pageable pageable = partitionsRequest.getPageable();
        final Map<String, ScanSummary.PartitionMetrics> partitionMap
            = icebergTableUtil.getIcebergTablePartitionMap(icebergTable, partitionsRequest);

        final List<PartitionInfo> filteredPartitionList;
        final List<String> partitionIds = partitionsRequest.getPartitionNames();
        final Sort sort = partitionsRequest.getSort();

        filteredPartitionList = partitionMap.keySet().stream()
            .filter(partitionName -> partitionIds == null || partitionIds.contains(partitionName))
            .map(partitionName -> PartitionInfo.builder().name(
                QualifiedName.ofPartition(tableName.getCatalogName(),
                    tableName.getDatabaseName(),
                    tableName.getTableName(),
                    partitionName))
                .dataMetrics(icebergTableUtil.getDataMetadataFromIcebergMetrics(partitionMap.get(partitionName)))
                .auditInfo(tableInfo.getAudit()).build())
            .collect(Collectors.toList());
        if (sort != null) {
            //it can only support sortBy partition Name
            final Comparator<PartitionInfo> nameComparator = Comparator.comparing(p -> p.getName().toString());
            ConnectorUtils.sort(filteredPartitionList, sort, nameComparator);
        }
        return ConnectorUtils.paginate(filteredPartitionList, pageable);
    }


}
