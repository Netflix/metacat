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
import com.google.common.base.Strings;
import com.google.common.cache.CacheLoader;
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
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveUtil.schemaTableName;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by amajumdar on 2/4/15.
 */
public class HiveSplitDetailManager extends HiveSplitManager implements ConnectorSplitDetailManager{

    protected final HiveMetastore metastore;
    protected final ConverterUtil converterUtil;
    @Inject
    public HiveSplitDetailManager(HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            HiveMetastore metastore,
            NamenodeStats namenodeStats, HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            @ForHiveClient
            ExecutorService executorService, ConverterUtil converterUtil) {
        super(connectorId, hiveClientConfig, metastore, namenodeStats, hdfsEnvironment, directoryLister,
                executorService);
        this.metastore = metastore;
        this.converterUtil = converterUtil;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, final String filterExpression
            , List<String> partitionIds, Sort sort, Pageable pageable, boolean includePartitionDetails) {
        SchemaTableName schemaTableName = HiveUtil.schemaTableName(table);
        List<ConnectorPartition> partitions = getPartitions( schemaTableName, filterExpression
                , partitionIds, sort, pageable, includePartitionDetails);
        return new ConnectorPartitionResult( partitions, TupleDomain.<ColumnHandle>none());
    }

    private List<ConnectorPartition> getPartitions(SchemaTableName schemaTableName, String filterExpression,
            List<String> partitionIds,
            Sort sort, Pageable pageable,
            boolean includePartitionDetails) {
        List<ConnectorPartition> result = getPartitions( schemaTableName, filterExpression, partitionIds);
        if( pageable != null && pageable.isPageable()){
            int limit = pageable.getOffset() + pageable.getLimit();
            if( result.size() < limit){
                limit = result.size();
            }
            if( pageable.getOffset() > limit) {
                result = Lists.newArrayList();
            } else {
                result = result.subList(pageable.getOffset(), limit);
            }
        }
        return result;
    }

    private List<ConnectorPartition> getPartitions(SchemaTableName schemaTableName, String filterExpression,
            List<String> partitionIds) {
        List<ConnectorPartition> result = Lists.newArrayList();
        List<String> queryPartitionIds = Lists.newArrayList();
        Table table = metastore.getTable( schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));
        Map<String,Partition> partitionMap = null;
        if (!Strings.isNullOrEmpty(filterExpression)) {
            Map<String,Partition> filteredPartitionMap = Maps.newHashMap();
            List<Partition> partitions = ((MetacatHiveMetastore)metastore).getPartitions( schemaTableName.getSchemaName(), schemaTableName.getTableName(), filterExpression);
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
            partitionMap = getPartitionsByNames(
                    schemaTableName.getSchemaName(), schemaTableName.getTableName(),
                    partitionIds);
        }
        Map<ColumnHandle, Comparable<?>> domainMap = ImmutableMap.of(new ColumnHandle(){}, "ignore");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withFixedValues(domainMap);
        final List<ConnectorPartition> finalResult = result;
        partitionMap.forEach((s, partition) -> {
            StorageDescriptor sd = partition.getSd();
            StorageInfo storageInfo = null;
            if (sd != null) {
                storageInfo = new StorageInfo();
                storageInfo.setUri(sd.getLocation());
                storageInfo.setInputFormat(sd.getInputFormat());
                storageInfo.setOutputFormat(sd.getOutputFormat());
                storageInfo.setParameters(sd.getParameters());
                SerDeInfo serDeInfo = sd.getSerdeInfo();
                if (serDeInfo != null) {
                    storageInfo.setSerializationLib(serDeInfo.getSerializationLib());
                    storageInfo.setSerdeInfoParameters(serDeInfo.getParameters());
                }
            }
            AuditInfo auditInfo = new AuditInfo();
            auditInfo.setCreatedDate((long) partition.getCreateTime());
            auditInfo.setLastUpdatedDate((long) partition.getLastAccessTime());
            finalResult.add(new ConnectorPartitionDetailImpl(s, tupleDomain, storageInfo, partition.getParameters(),
                    auditInfo));
        });
        return result;
    }

    protected Map<String,Partition> getPartitionsByNames(String schemaName, String tableName, List<String> partitionNames)
    {
        try {
            return metastore.getPartitionsByNames(schemaName, tableName, partitionNames).orElse(Collections.emptyMap());
        } catch (CacheLoader.InvalidCacheLoadException ignored) {
            // Ignore cache failure
            return Collections.emptyMap();
        }
    }

    @Override
    public SavePartitionResult savePartitions(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions
            , List<String> partitionIdsForDeletes, boolean checkIfExists, boolean alterIfExists) {
        checkNotNull(tableHandle, "tableHandle is null");
        SavePartitionResult result = new SavePartitionResult();
        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> oTable = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        Table table = oTable.orElseThrow(() -> new TableNotFoundException(tableName));
        try {
            // New partition ids
            List<String> addedPartitionIds = Lists.newArrayList();
            // Updated partition ids
            List<String> existingPartitionIds = Lists.newArrayList();
            // Existing partitions
            List<Partition> existingHivePartitions = Lists.newArrayList();
            // New partitions
            List<Partition> hivePartitions = Lists.newArrayList();
            // Existing partition map
            Map<String,Partition> existingPartitionMap = Collections.emptyMap();
            if( checkIfExists) {
                List<String> partitionNames = partitions.stream().map(
                        partition -> {
                            String partitionName = partition.getPartitionId();
                            PartitionUtil
                                    .validatePartitionName(partitionName, getPartitionKeys(table.getPartitionKeys()));
                            return partitionName;
                        }).collect(Collectors.toList());
                existingPartitionMap = getPartitionsByNames(tableName.getSchemaName(), tableName.getTableName(),
                        partitionNames);
            }
            for(ConnectorPartition partition:partitions){
                String partitionName = partition.getPartitionId();
                Partition hivePartition = existingPartitionMap.get(partitionName);
                if(hivePartition == null){
                    addedPartitionIds.add(partitionName);
                    hivePartitions.add(converterUtil.toPartition(tableName, partition));
                } else {
                    ConnectorPartitionDetail partitionDetail = (ConnectorPartitionDetail) partition;
                    String partitionUri = getUri(partitionDetail);
                    String hivePartitionUri = getUri(hivePartition);
                    if( partitionUri == null || !partitionUri.equals( hivePartitionUri)){
                        existingPartitionIds.add(partitionName);
                        Partition existingPartition = converterUtil.toPartition(tableName, partition);
                        if( alterIfExists){
                            existingPartition.setParameters(hivePartition.getParameters());
                            existingPartition.setCreateTime(hivePartition.getCreateTime());
                            existingHivePartitions.add(existingPartition);
                        } else {
                            hivePartitions.add(existingPartition);
                        }
                    }
                }
            }
            Set<String> deletePartitionIds = Sets.newHashSet();
            if( !alterIfExists) {
                deletePartitionIds.addAll(existingPartitionIds);
            }
            if( partitionIdsForDeletes != null){
                deletePartitionIds.addAll(partitionIdsForDeletes);
            }

            if( alterIfExists && !existingHivePartitions.isEmpty()){
                copyTableSdToPartitionSd( existingHivePartitions, table);
                ((MetacatHiveMetastore)metastore).alterPartitions(tableName.getSchemaName()
                        , tableName.getTableName(), existingHivePartitions);
            }
            copyTableSdToPartitionSd( hivePartitions, table);
            ((MetacatHiveMetastore)metastore).addDropPartitions(tableName.getSchemaName()
                    , tableName.getTableName(), hivePartitions, Lists.newArrayList(deletePartitionIds));

            result.setAdded( addedPartitionIds);
            result.setUpdated( existingPartitionIds);

            // If partitions were added or changed we have to flush the cache so they will show in listing immediately
            if (!result.getAdded().isEmpty() || !result.getUpdated().isEmpty()) {
                metastore.flushCache();
            }
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        } catch (AlreadyExistsException e) {
            throw new PartitionAlreadyExistsException(tableName, null, e);
        }
        return result;
    }

    private void copyTableSdToPartitionSd(List<Partition> hivePartitions, Table table) {
        //
        // Update the partition info based on that of the table.
        //
        for (Partition partition : hivePartitions) {
            StorageDescriptor sd = partition.getSd();
            StorageDescriptor tableSdCopy = table.getSd().deepCopy();
            if (tableSdCopy.getSerdeInfo() == null) {
                SerDeInfo serDeInfo = new SerDeInfo(null, null, Collections.emptyMap());
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

    private String getUri(Partition hivePartition) {
        String result = null;
        if( hivePartition.getSd() != null){
            result = hivePartition.getSd().getLocation();
        }
        return result;
    }

    private String getUri(ConnectorPartitionDetail partitionDetail) {
        String result = null;
        if( partitionDetail.getStorageInfo() != null){
            result = partitionDetail.getStorageInfo().getUri();
        }
        return result;
    }

    public List<String> getPartitionKeys(List<FieldSchema> fields) {
        List<String> result = Lists.newArrayList();
        if (fields != null) {
            result.addAll(fields.stream().map(FieldSchema::getName).collect(Collectors.toList()));
        }
        return result;
    }

    /**
     * Delete partitions for a table

     * @param tableHandle table handle
     * @param partitionIds list of partition names
     */
    @Override
    public void deletePartitions(ConnectorTableHandle tableHandle, List<String> partitionIds) {
        if (!(metastore instanceof MetacatHiveMetastore)) {
            throw new IllegalStateException("This metastore does not implement dropPartitions");
        }
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = schemaTableName(tableHandle);
        try {
            ((MetacatHiveMetastore)metastore).dropPartitions( tableName.getSchemaName(), tableName.getTableName(), partitionIds);
            // Flush the cache after deleting partitions
            metastore.flushCache();
        } catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(tableName, partitionIds.toString());
        }
    }

    /**
     * Number of partitions for the given table
     * @param connectorHandle table handle
     * @return Number of partitions
     */
    @Override
    public Integer getPartitionCount(ConnectorTableHandle connectorHandle) {
        SchemaTableName schemaTableName = HiveUtil.schemaTableName(connectorHandle);
        return metastore.getPartitionNames(schemaTableName.getSchemaName(), schemaTableName.getTableName()).orElse(Lists.newArrayList()).size();
    }

    public List<String> getPartitionKeys(ConnectorTableHandle tableHandle, String filterExpression, List<String> partitionNames, Sort sort, Pageable pageable){
        List<String> result = null;
        SchemaTableName schemaTableName = HiveUtil.schemaTableName(tableHandle);
        if( filterExpression != null || (partitionNames != null && !partitionNames.isEmpty())){
            result = getPartitions(schemaTableName, filterExpression, partitionNames).stream().map(
                    ConnectorPartition::getPartitionId).collect(Collectors.toList());
        } else {
            result = metastore.getPartitionNames(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                    .orElse(Lists.newArrayList());
        }
        if( pageable != null && pageable.isPageable()){
            int limit = pageable.getOffset() + pageable.getLimit();
            if( result.size() < limit){
                limit = result.size();
            }
            if( pageable.getOffset() > limit) {
                result = Lists.newArrayList();
            } else {
                result = result.subList(pageable.getOffset(), limit);
            }
        }
        return result;
    }

    public List<String> getPartitionUris(ConnectorTableHandle table, String filterExpression, List<String> partitionNames, Sort sort, Pageable pageable){
        SchemaTableName schemaTableName = HiveUtil.schemaTableName(table);
        return getPartitions(schemaTableName, filterExpression, partitionNames, sort, pageable, true).stream().map(
                partition -> ((ConnectorPartitionDetail) partition).getStorageInfo().getUri()).collect(Collectors.toList());
    }
}
