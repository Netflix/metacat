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

package com.netflix.metacat.hive.connector;

import com.facebook.presto.exception.InvalidMetaException;
import com.facebook.presto.exception.PartitionAlreadyExistsException;
import com.facebook.presto.exception.PartitionNotFoundException;
import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.ForHiveClient;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.HiveSplitManager;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.AuditInfo;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionDetail;
import com.facebook.presto.spi.ConnectorPartitionDetailImpl;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitDetailManager;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.SavePartitionResult;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Sort;
import com.facebook.presto.spi.StorageInfo;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.partition.util.PartitionUtil;
import com.netflix.metacat.hive.connector.util.ConverterUtil;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Hive split detail manager.
 */
public class HiveSplitDetailManager extends HiveSplitManager implements ConnectorSplitDetailManager {

    protected final HiveMetastore metastore;
    protected final ConverterUtil converterUtil;

    /**
     * Constructor.
     * @param connectorId connector id
     * @param hiveClientConfig config
     * @param metastore metastore
     * @param namenodeStats stats
     * @param hdfsEnvironment hdfs
     * @param directoryLister directory lister
     * @param executorService executor
     * @param converterUtil util
     */
    @Inject
    public HiveSplitDetailManager(final HiveConnectorId connectorId,
        final HiveClientConfig hiveClientConfig,
        final HiveMetastore metastore,
        final NamenodeStats namenodeStats, final HdfsEnvironment hdfsEnvironment,
        final DirectoryLister directoryLister,
        @ForHiveClient
        final ExecutorService executorService, final ConverterUtil converterUtil) {
        super(connectorId, hiveClientConfig, metastore, namenodeStats, hdfsEnvironment, directoryLister,
            executorService);
        this.metastore = metastore;
        this.converterUtil = converterUtil;
    }

    @Override
    public ConnectorPartitionResult getPartitions(final ConnectorTableHandle table, final String filterExpression,
        final List<String> partitionIds, final Sort sort, final Pageable pageable,
        final boolean includePartitionDetails) {
        final SchemaTableName schemaTableName = HiveUtil.schemaTableName(table);
        final List<ConnectorPartition> partitions = getPartitions(schemaTableName, filterExpression,
            partitionIds, sort, pageable, includePartitionDetails);
        return new ConnectorPartitionResult(partitions, TupleDomain.none());
    }

    private List<ConnectorPartition> getPartitions(final SchemaTableName schemaTableName, final String filterExpression,
        final List<String> partitionIds,
        final Sort sort, final Pageable pageable,
        final boolean includePartitionDetails) {
        List<ConnectorPartition> result = getPartitions(schemaTableName, filterExpression, partitionIds);
        if (pageable != null && pageable.isPageable()) {
            int limit = pageable.getOffset() + pageable.getLimit();
            if (result.size() < limit) {
                limit = result.size();
            }
            if (pageable.getOffset() > limit) {
                result = Lists.newArrayList();
            } else {
                result = result.subList(pageable.getOffset(), limit);
            }
        }
        return result;
    }

    private List<ConnectorPartition> getPartitions(final SchemaTableName schemaTableName, final String filterExpression,
        final List<String> partitionIds) {
        final List<ConnectorPartition> result = Lists.newArrayList();
        final Table table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName())
            .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        Map<String, Partition> partitionMap = null;
        if (!Strings.isNullOrEmpty(filterExpression)) {
            final Map<String, Partition> filteredPartitionMap = Maps.newHashMap();
            final List<Partition> partitions = ((MetacatHiveMetastore) metastore)
                .getPartitions(schemaTableName.getSchemaName(), schemaTableName.getTableName(), filterExpression);
            partitions.forEach(partition -> {
                String partitionName = null;
                try {
                    partitionName = Warehouse.makePartName(table.getPartitionKeys(), partition.getValues());
                } catch (Exception e) {
                    throw new InvalidMetaException("One or more partition names are invalid.", e);
                }
                if (partitionIds == null || partitionIds.contains(partitionName)) {
                    filteredPartitionMap.put(partitionName, partition);
                }
            });
            partitionMap = filteredPartitionMap;
        } else {
            partitionMap = getPartitionsByNames(table, partitionIds);
        }
        final Map<ColumnHandle, Comparable<?>> domainMap = ImmutableMap.of(new ColumnHandle() {
        }, "ignore");
        final TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withFixedValues(domainMap);
        final List<ConnectorPartition> finalResult = result;
        partitionMap.forEach((s, partition) -> {
            final StorageDescriptor sd = partition.getSd();
            StorageInfo storageInfo = null;
            if (sd != null) {
                storageInfo = new StorageInfo();
                storageInfo.setUri(sd.getLocation());
                storageInfo.setInputFormat(sd.getInputFormat());
                storageInfo.setOutputFormat(sd.getOutputFormat());
                storageInfo.setParameters(sd.getParameters());
                final SerDeInfo serDeInfo = sd.getSerdeInfo();
                if (serDeInfo != null) {
                    storageInfo.setSerializationLib(serDeInfo.getSerializationLib());
                    storageInfo.setSerdeInfoParameters(serDeInfo.getParameters());
                }
            }
            final AuditInfo auditInfo = new AuditInfo();
            auditInfo.setCreatedDate((long) partition.getCreateTime());
            auditInfo.setLastUpdatedDate((long) partition.getLastAccessTime());
            finalResult.add(new ConnectorPartitionDetailImpl(s, tupleDomain, storageInfo, partition.getParameters(),
                auditInfo));
        });
        return result;
    }

    protected Map<String, Partition> getPartitionsByNames(final Table table, final List<String> partitionNames) {
        List<Partition> partitions =
            ((MetacatHiveMetastore) metastore).getPartitions(table.getDbName(), table.getTableName(),
                partitionNames);
        if (partitions == null || partitions.isEmpty()) {
            if (partitionNames == null || partitionNames.isEmpty()) {
                return Collections.emptyMap();
            }

            // Fall back to scanning all partitions ourselves if finding by name does not work
            final List<Partition> allPartitions =
                ((MetacatHiveMetastore) metastore).getPartitions(table.getDbName(), table.getTableName(),
                    Collections.emptyList());
            if (allPartitions == null || allPartitions.isEmpty()) {
                return Collections.emptyMap();
            }

            partitions = allPartitions.stream().filter(part -> {
                try {
                    return partitionNames.contains(Warehouse.makePartName(table.getPartitionKeys(), part.getValues()));
                } catch (Exception e) {
                    throw new InvalidMetaException("One or more partition names are invalid.", e);
                }
            }).collect(Collectors.toList());
        }

        return partitions.stream().collect(Collectors.toMap(part -> {
            try {
                return Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
            } catch (Exception e) {
                throw new InvalidMetaException("One or more partition names are invalid.", e);
            }
        }, Function.identity()));
    }

    @Override
    public SavePartitionResult savePartitions(final ConnectorTableHandle tableHandle,
        final List<ConnectorPartition> partitions, final List<String> partitionIdsForDeletes,
        final boolean checkIfExists, final boolean alterIfExists) {
        Preconditions.checkNotNull(tableHandle, "tableHandle is null");
        final SavePartitionResult result = new SavePartitionResult();
        final SchemaTableName tableName = HiveUtil.schemaTableName(tableHandle);
        final Optional<Table> oTable = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        final Table table = oTable.orElseThrow(() -> new TableNotFoundException(tableName));
        try {
            // New partition ids
            final List<String> addedPartitionIds = Lists.newArrayList();
            // Updated partition ids
            final List<String> existingPartitionIds = Lists.newArrayList();
            // Existing partitions
            final List<Partition> existingHivePartitions = Lists.newArrayList();
            // New partitions
            final List<Partition> hivePartitions = Lists.newArrayList();
            // Existing partition map
            Map<String, Partition> existingPartitionMap = Collections.emptyMap();
            if (checkIfExists) {
                final List<String> partitionNames = partitions.stream().map(
                    partition -> {
                        final String partitionName = partition.getPartitionId();
                        PartitionUtil
                            .validatePartitionName(partitionName, getPartitionKeys(table.getPartitionKeys()));
                        return partitionName;
                    }).collect(Collectors.toList());
                existingPartitionMap = getPartitionsByNames(table, partitionNames);
            }
            for (ConnectorPartition partition : partitions) {
                final String partitionName = partition.getPartitionId();
                final Partition hivePartition = existingPartitionMap.get(partitionName);
                if (hivePartition == null) {
                    addedPartitionIds.add(partitionName);
                    hivePartitions.add(converterUtil.toPartition(tableName, partition));
                } else {
                    final ConnectorPartitionDetail partitionDetail = (ConnectorPartitionDetail) partition;
                    final String partitionUri = getUri(partitionDetail);
                    final String hivePartitionUri = getUri(hivePartition);
                    if (partitionUri == null || !partitionUri.equals(hivePartitionUri)) {
                        existingPartitionIds.add(partitionName);
                        final Partition existingPartition = converterUtil.toPartition(tableName, partition);
                        if (alterIfExists) {
                            existingPartition.setParameters(hivePartition.getParameters());
                            existingPartition.setCreateTime(hivePartition.getCreateTime());
                            existingHivePartitions.add(existingPartition);
                        } else {
                            hivePartitions.add(existingPartition);
                        }
                    }
                }
            }
            final Set<String> deletePartitionIds = Sets.newHashSet();
            if (!alterIfExists) {
                deletePartitionIds.addAll(existingPartitionIds);
            }
            if (partitionIdsForDeletes != null) {
                deletePartitionIds.addAll(partitionIdsForDeletes);
            }

            if (alterIfExists && !existingHivePartitions.isEmpty()) {
                copyTableSdToPartitionSd(existingHivePartitions, table);
                ((MetacatHiveMetastore) metastore).alterPartitions(tableName.getSchemaName(),
                    tableName.getTableName(), existingHivePartitions);
            }
            copyTableSdToPartitionSd(hivePartitions, table);
            ((MetacatHiveMetastore) metastore).addDropPartitions(tableName.getSchemaName(),
                tableName.getTableName(), hivePartitions, Lists.newArrayList(deletePartitionIds));

            result.setAdded(addedPartitionIds);
            result.setUpdated(existingPartitionIds);

            // If partitions were added or changed we have to flush the cache so they will show in listing immediately
            if (!result.getAdded().isEmpty() || !result.getUpdated().isEmpty()) {
                metastore.flushCache();
            }
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        } catch (AlreadyExistsException e) {
            final List<String> ids = partitions.stream()
                .map(ConnectorPartition::getPartitionId).collect(Collectors.toList());
            throw new PartitionAlreadyExistsException(tableName, ids, e);
        }
        return result;
    }

    private void copyTableSdToPartitionSd(final List<Partition> hivePartitions, final Table table) {
        //
        // Update the partition info based on that of the table.
        //
        for (Partition partition : hivePartitions) {
            final StorageDescriptor sd = partition.getSd();
            final StorageDescriptor tableSdCopy = table.getSd().deepCopy();
            if (tableSdCopy.getSerdeInfo() == null) {
                final SerDeInfo serDeInfo = new SerDeInfo(null, null, Collections.emptyMap());
                tableSdCopy.setSerdeInfo(serDeInfo);
            }

            tableSdCopy.setLocation(sd.getLocation());
            if (!Strings.isNullOrEmpty(sd.getInputFormat())) {
                tableSdCopy.setInputFormat(sd.getInputFormat());
            }
            if (!Strings.isNullOrEmpty(sd.getOutputFormat())) {
                tableSdCopy.setOutputFormat(sd.getOutputFormat());
            }
            if (sd.getParameters() != null && !sd.getParameters().isEmpty()) {
                tableSdCopy.setParameters(sd.getParameters());
            }
            if (sd.getSerdeInfo() != null) {
                if (!Strings.isNullOrEmpty(sd.getSerdeInfo().getName())) {
                    tableSdCopy.getSerdeInfo().setName(sd.getSerdeInfo().getName());
                }
                if (!Strings.isNullOrEmpty(sd.getSerdeInfo().getSerializationLib())) {
                    tableSdCopy.getSerdeInfo().setSerializationLib(sd.getSerdeInfo().getSerializationLib());
                }
                if (sd.getSerdeInfo().getParameters() != null && !sd.getSerdeInfo().getParameters().isEmpty()) {
                    tableSdCopy.getSerdeInfo().setParameters(sd.getSerdeInfo().getParameters());
                }
            }
            partition.setSd(tableSdCopy);
        }
    }

    private String getUri(final Partition hivePartition) {
        String result = null;
        if (hivePartition.getSd() != null) {
            result = hivePartition.getSd().getLocation();
        }
        return result;
    }

    private String getUri(final ConnectorPartitionDetail partitionDetail) {
        String result = null;
        if (partitionDetail.getStorageInfo() != null) {
            result = partitionDetail.getStorageInfo().getUri();
        }
        return result;
    }

    /**
     * Returns the list of partition keys.
     * @param fields fields
     * @return partition keys
     */
    public List<String> getPartitionKeys(final List<FieldSchema> fields) {
        final List<String> result = Lists.newArrayList();
        if (fields != null) {
            result.addAll(fields.stream().map(FieldSchema::getName).collect(Collectors.toList()));
        }
        return result;
    }

    /**
     * Delete partitions for a table.
     *
     * @param tableHandle table handle
     * @param partitionIds list of partition names
     */
    @Override
    public void deletePartitions(final ConnectorTableHandle tableHandle, final List<String> partitionIds) {
        if (!(metastore instanceof MetacatHiveMetastore)) {
            throw new IllegalStateException("This metastore does not implement dropPartitions");
        }
        Preconditions.checkNotNull(tableHandle, "tableHandle is null");
        final SchemaTableName tableName = HiveUtil.schemaTableName(tableHandle);
        try {
            ((MetacatHiveMetastore) metastore)
                .dropPartitions(tableName.getSchemaName(), tableName.getTableName(), partitionIds);
            // Flush the cache after deleting partitions
            metastore.flushCache();
        } catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(tableName, partitionIds.toString());
        }
    }

    /**
     * Number of partitions for the given table.
     * @param connectorHandle table handle
     * @return Number of partitions
     */
    @Override
    public Integer getPartitionCount(final ConnectorTableHandle connectorHandle) {
        final SchemaTableName schemaTableName = HiveUtil.schemaTableName(connectorHandle);
        return metastore.getPartitionNames(schemaTableName.getSchemaName(), schemaTableName.getTableName())
            .orElse(Lists.newArrayList()).size();
    }

    @Override
    public List<String> getPartitionKeys(final ConnectorTableHandle tableHandle, final String filterExpression,
        final List<String> partitionNames, final Sort sort, final Pageable pageable) {
        List<String> result = null;
        final SchemaTableName schemaTableName = HiveUtil.schemaTableName(tableHandle);
        if (filterExpression != null || (partitionNames != null && !partitionNames.isEmpty())) {
            result = getPartitions(schemaTableName, filterExpression, partitionNames).stream().map(
                ConnectorPartition::getPartitionId).collect(Collectors.toList());
        } else {
            result = metastore.getPartitionNames(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElse(Lists.newArrayList());
        }
        if (pageable != null && pageable.isPageable()) {
            int limit = pageable.getOffset() + pageable.getLimit();
            if (result.size() < limit) {
                limit = result.size();
            }
            if (pageable.getOffset() > limit) {
                result = Lists.newArrayList();
            } else {
                result = result.subList(pageable.getOffset(), limit);
            }
        }
        return result;
    }

    @Override
    public List<String> getPartitionUris(final ConnectorTableHandle table, final String filterExpression,
        final List<String> partitionNames, final Sort sort, final Pageable pageable) {
        final SchemaTableName schemaTableName = HiveUtil.schemaTableName(table);
        return getPartitions(schemaTableName, filterExpression, partitionNames, sort, pageable, true).stream().map(
            partition -> ((ConnectorPartitionDetail) partition).getStorageInfo().getUri()).collect(Collectors.toList());
    }
}
