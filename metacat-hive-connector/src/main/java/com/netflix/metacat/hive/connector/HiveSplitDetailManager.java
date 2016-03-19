package com.netflix.metacat.hive.connector;

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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.partition.util.PartitionUtil;
import com.netflix.metacat.hive.connector.util.ConverterUtil;
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
    @Inject
    public HiveSplitDetailManager(HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            HiveMetastore metastore,
            NamenodeStats namenodeStats, HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            @ForHiveClient
            ExecutorService executorService) {
        super(connectorId, hiveClientConfig, metastore, namenodeStats, hdfsEnvironment, directoryLister,
                executorService);
        this.metastore = metastore;
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
        List<ConnectorPartition> result = Lists.newArrayList();
        List<String> queryPartitionIds = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(filterExpression)) {
            queryPartitionIds = metastore
                    .getPartitionNamesByParts(schemaTableName.getSchemaName(), schemaTableName.getTableName(),
                            Lists.newArrayList(PartitionUtil.getPartitionKeyValues(filterExpression).values())).orElse(Lists.newArrayList());
        }
        if (partitionIds != null) {
            queryPartitionIds.addAll(partitionIds);
        } else {
            queryPartitionIds.addAll(metastore.getPartitionNames(schemaTableName.getSchemaName(),
                    schemaTableName.getTableName()).orElse(Lists.newArrayList()));
        }
        Map<String,Partition> partitionMap = getPartitionsByNames(
                schemaTableName.getSchemaName(), schemaTableName.getTableName(),
                queryPartitionIds);
        Map<ColumnHandle, Comparable<?>> domainMap = Maps.newHashMapWithExpectedSize(1);
        domainMap.put(new ColumnHandle(){}, "ignore");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withFixedValues(domainMap);
        partitionMap.forEach((s, partition) -> {
            StorageDescriptor sd = partition.getSd();
            StorageInfo storageInfo = null;
            if( sd != null){
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
            auditInfo.setCreatedDate((long)partition.getCreateTime());
            auditInfo.setLastUpdatedDate((long) partition.getLastAccessTime());
            result.add( new ConnectorPartitionDetailImpl(s, tupleDomain, storageInfo, partition.getParameters(), auditInfo));
        });
        return result;
    }

    protected Map<String,Partition> getPartitionsByNames(String schemaName, String tableName, List<String> partitionNames)
    {
        return metastore.getPartitionsByNames(schemaName, tableName, partitionNames).orElse(Maps.newHashMap());
    }

    @Override
    public SavePartitionResult savePartitions(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions
            , List<String> partitionIdsForDeletes, boolean checkIfExists) {
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
            // New partitions
            List<Partition> hivePartitions = Lists.newArrayList();
            // Existing partition map
            Map<String,Partition> existingPartitionMap = Maps.newHashMap();
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
                    hivePartitions.add(ConverterUtil.toPartition(tableName, partition));
                } else {
                    ConnectorPartitionDetail partitionDetail = (ConnectorPartitionDetail) partition;
                    String partitionUri = getUri(partitionDetail);
                    String hivePartitionUri = getUri(hivePartition);
                    if( partitionUri == null || !partitionUri.equals( hivePartitionUri)){
                        existingPartitionIds.add(partitionName);
                        hivePartitions.add(ConverterUtil.toPartition(tableName, partition));
                    }
                }
            }
            Set<String> deletePartitionIds = Sets.newHashSet();
            deletePartitionIds.addAll(existingPartitionIds);
            if( partitionIdsForDeletes != null){
                deletePartitionIds.addAll(partitionIdsForDeletes);
            }

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
                if (sd.getParameters() != null) {
                    tableSdCopy.setParameters(sd.getParameters());
                }
                if (sd.getSerdeInfo() != null) {
                    if (!Strings.isNullOrEmpty(sd.getSerdeInfo().getName())) {
                        tableSdCopy.getSerdeInfo().setName(sd.getSerdeInfo().getName());
                    }
                    if (!Strings.isNullOrEmpty(sd.getSerdeInfo().getSerializationLib())) {
                        tableSdCopy.getSerdeInfo().setSerializationLib(sd.getSerdeInfo().getSerializationLib());
                    }
                    if (sd.getSerdeInfo().getParameters() != null) {
                        tableSdCopy.getSerdeInfo().setParameters(sd.getSerdeInfo().getParameters());
                    }
                }
                partition.setSd(tableSdCopy);
            }

            ((MetacatHiveMetastore)metastore).addDropPartitions(tableName.getSchemaName()
                    , tableName.getTableName(), hivePartitions, Lists.newArrayList(deletePartitionIds));

            result.setAdded( addedPartitionIds);
            result.setUpdated( existingPartitionIds);
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        } catch (AlreadyExistsException e) {
            throw new PartitionAlreadyExistsException(tableName, null, e);
        }
        return result;
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
}
