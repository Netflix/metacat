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

package com.netflix.metacat.connector.hive;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.ConnectorException;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.exception.PartitionAlreadyExistsException;
import com.netflix.metacat.common.server.connectors.exception.PartitionNotFoundException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveResponse;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.partition.util.PartitionUtil;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.sql.PartitionHolder;
import lombok.Getter;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * HiveConnectorPartitionService.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Getter
public class HiveConnectorPartitionService implements ConnectorPartitionService {
    private final String catalogName;
    private final ConnectorContext context;
    private final HiveConnectorInfoConverter hiveMetacatConverters;
    private final IMetacatHiveClient metacatHiveClient;


    /**
     * Constructor.
     *
     * @param context               connector context
     * @param metacatHiveClient     hive client
     * @param hiveMetacatConverters hive converter
     */
    public HiveConnectorPartitionService(
        final ConnectorContext context,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters
    ) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
        this.catalogName = context.getCatalogName();
        this.context = context;
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
        try {
            final List<Partition> partitions = getPartitions(tableName,
                partitionsRequest.getFilter(), partitionsRequest.getPartitionNames(),
                partitionsRequest.getSort(), partitionsRequest.getPageable());
            final Table table = metacatHiveClient.getTableByName(tableName.getDatabaseName(), tableName.getTableName());
            final TableInfo tableInfo = hiveMetacatConverters.toTableInfo(tableName, table);
            final List<PartitionInfo> partitionInfos = new ArrayList<>();
            for (Partition partition : partitions) {
                partitionInfos.add(hiveMetacatConverters.toPartitionInfo(tableInfo, partition));
            }
            return partitionInfos;
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(tableName, exception);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + tableName, e);
        } catch (TException e) {
            throw new ConnectorException(String.format("Failed get partitions for hive table %s", tableName), e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int getPartitionCount(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName
    ) {
        try {
            return metacatHiveClient.getPartitionCount(tableName.getDatabaseName(), tableName.getTableName());
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(tableName, exception);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + tableName, e);
        } catch (TException e) {
            throw new ConnectorException(String.format("Failed get partitions count for hive table %s", tableName), e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionKeys(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName,
        final PartitionListRequest partitionsRequest
    ) {
        final String filterExpression = partitionsRequest.getFilter();
        final List<String> partitionIds = partitionsRequest.getPartitionNames();
        List<String> names = Lists.newArrayList();
        final Pageable pageable = partitionsRequest.getPageable();
        try {
            if (filterExpression != null || (partitionIds != null && !partitionIds.isEmpty())) {
                final Table table = metacatHiveClient.getTableByName(tableName.getDatabaseName(),
                    tableName.getTableName());
                for (Partition partition : getPartitions(tableName, filterExpression,
                    partitionIds, partitionsRequest.getSort(), pageable)) {
                    names.add(getNameOfPartition(table, partition));
                }
            } else {
                names = metacatHiveClient.getPartitionNames(tableName.getDatabaseName(), tableName.getTableName());
                return ConnectorUtils.paginate(names, pageable);
            }
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(tableName, exception);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + tableName, e);
        } catch (TException e) {
            throw new ConnectorException(String.format("Failed get partitions keys for hive table %s", tableName), e);
        }
        return names;
    }

    private List<Partition> getPartitions(
        final QualifiedName tableName,
        @Nullable final String filter,
        @Nullable final List<String> partitionIds,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        final String databasename = tableName.getDatabaseName();
        final String tablename = tableName.getTableName();
        try {
            final Table table = metacatHiveClient.getTableByName(databasename, tablename);
            List<Partition> partitionList = null;
            if (!Strings.isNullOrEmpty(filter)) {
                partitionList = metacatHiveClient.listPartitionsByFilter(databasename,
                    tablename, filter);
            } else {
                if (partitionIds != null) {
                    partitionList = metacatHiveClient.getPartitions(databasename,
                        tablename, partitionIds);
                }
                if (partitionList == null || partitionList.isEmpty()) {
                    partitionList = metacatHiveClient.getPartitions(databasename,
                        tablename, null);
                }
            }
            final List<Partition> filteredPartitionList = Lists.newArrayList();
            partitionList.forEach(partition -> {
                final String partitionName = getNameOfPartition(table, partition);
                if (partitionIds == null || partitionIds.contains(partitionName)) {
                    filteredPartitionList.add(partition);
                }
            });
            if (sort != null) {
                if (sort.getOrder() == SortOrder.DESC) {
                    filteredPartitionList.sort(Collections.reverseOrder());
                } else {
                    Collections.sort(filteredPartitionList);
                }
            }
            return ConnectorUtils.paginate(filteredPartitionList, pageable);
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(tableName, exception);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + tableName, e);
        } catch (TException e) {
            throw new ConnectorException(String.format("Failed get partitions for hive table %s", tableName), e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionUris(
        final ConnectorRequestContext requestContext,
        final QualifiedName table,
        final PartitionListRequest partitionsRequest
    ) {
        final List<String> uris = Lists.newArrayList();
        for (Partition partition : getPartitions(table, partitionsRequest.getFilter(),
            partitionsRequest.getPartitionNames(), partitionsRequest.getSort(), partitionsRequest.getPageable())) {
            uris.add(partition.getSd().getLocation());
        }
        return uris;
    }

    /**
     * By default(checkIfExists=true and aletrIfExists=false), this method adds the provided list of partitions.
     * If a partition already exists, it is dropped first before adding it.
     * If checkIfExists=false, the method adds the partitions to the table. If a partition already exists,
     * an AlreadyExistsException error is thrown.
     * If alterIfExists=true, the method updates existing partitions and adds non-existant partitions.
     *
     * If a partition in the provided partition list has all the details, then it is used. If the details are missing,
     * then the table details are inherited. This is mostly for the storage information.
     */
    @Override
    public PartitionsSaveResponse savePartitions(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableQName,
        final PartitionsSaveRequest partitionsSaveRequest
    ) {
        final String databaseName = tableQName.getDatabaseName();
        final String tableName = tableQName.getTableName();
        final Table table;
        try {
            table = metacatHiveClient.getTableByName(databaseName, tableName);
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(tableQName, exception);
        } catch (TException e) {
            throw new ConnectorException(String.format("Failed getting hive table %s", tableQName), e);
        }
        // New partitions
        final List<PartitionInfo> addedPartitionInfos = Lists.newArrayList();
        final List<PartitionInfo> partitionInfos = partitionsSaveRequest.getPartitions();
        final List<String> partitionNames = partitionInfos.stream()
            .map(part  -> {
                final String partitionName = part.getName().getPartitionName();
                PartitionUtil.validatePartitionName(partitionName, getPartitionKeys(table.getPartitionKeys()));
                return partitionName;
            }).collect(Collectors.toList());

        // New partition names
        final List<String> addedPartitionNames = Lists.newArrayList();
        // Updated partition names
        final List<String> existingPartitionNames = Lists.newArrayList();
        // Existing partitions
        final List<PartitionHolder> existingPartitionHolders = Lists.newArrayList();

        // Existing partition map
        Map<String, PartitionHolder> existingPartitionMap = Collections.emptyMap();

        //
        // If either checkIfExists or alterIfExists is true, check to see if any of the partitions already exists.
        // If it exists and if alterIfExists=false, we will drop it before adding.
        // If it exists and if alterIfExists=true, we will alter it.
        //
        if (partitionsSaveRequest.getCheckIfExists() || partitionsSaveRequest.getAlterIfExists()) {
            existingPartitionMap = getPartitionsByNames(table, partitionNames);
        }

        for (PartitionInfo partitionInfo : partitionInfos) {
            final String partitionName = partitionInfo.getName().getPartitionName();
            final PartitionHolder existingPartitionHolder = existingPartitionMap.get(partitionName);
            if (existingPartitionHolder == null) {
                addedPartitionNames.add(partitionName);
                addedPartitionInfos.add(partitionInfo);
            } else {
                final String partitionUri =
                    partitionInfo.getSerde() != null ? partitionInfo.getSerde().getUri() : null;
                final String existingPartitionUri = getPartitionUri(existingPartitionHolder);
                if (partitionUri == null || !partitionUri.equals(existingPartitionUri)) {
                    existingPartitionNames.add(partitionName);
                    //the partition exists, we should not do anything for the partition exists
                    //unless we alterifExists
                    if (partitionsSaveRequest.getAlterIfExists()) {
                        if (partitionInfo.getSerde() == null) {
                            partitionInfo.setSerde(new StorageInfo());
                        }
                        if (partitionInfo.getAudit() == null) {
                            partitionInfo.setAudit(new AuditInfo());
                        }
                        if (existingPartitionHolder.getPartition() != null) {
                            final Partition existingPartition = existingPartitionHolder.getPartition();
                            partitionInfo.getSerde().setParameters(existingPartition.getParameters());
                            partitionInfo.getAudit().setCreatedDate(
                                HiveConnectorInfoConverter.epochSecondsToDate(existingPartition.getCreateTime()));
                            partitionInfo.getAudit().setLastModifiedDate(
                                HiveConnectorInfoConverter.epochSecondsToDate(existingPartition.getLastAccessTime()));
                        } else {
                            final PartitionInfo existingPartitionInfo = existingPartitionHolder.getPartitionInfo();
                            if (existingPartitionInfo.getSerde() != null) {
                                partitionInfo.getSerde()
                                    .setParameters(existingPartitionInfo.getSerde().getParameters());
                            }
                            if (existingPartitionInfo.getAudit() != null) {
                                partitionInfo.getAudit()
                                    .setCreatedDate(existingPartitionInfo.getAudit().getCreatedDate());
                                partitionInfo.getAudit()
                                    .setLastModifiedDate(existingPartitionInfo.getAudit().getLastModifiedDate());
                            }
                        }
                        existingPartitionHolder.setPartitionInfo(partitionInfo);
                        existingPartitionHolders.add(existingPartitionHolder);
                    } else {
                        addedPartitionInfos.add(partitionInfo);
                    }
                }
            }
        }

        final Set<String> deletePartitionNames = Sets.newHashSet();
        if (!partitionsSaveRequest.getAlterIfExists()) {
            deletePartitionNames.addAll(existingPartitionNames);
        }
        if (partitionsSaveRequest.getPartitionIdsForDeletes() != null) {
            deletePartitionNames.addAll(partitionsSaveRequest.getPartitionIdsForDeletes());
        }

        addUpdateDropPartitions(tableQName, table, partitionNames, addedPartitionInfos, existingPartitionHolders,
            deletePartitionNames);

        final PartitionsSaveResponse result = new PartitionsSaveResponse();
        result.setAdded(addedPartitionNames);
        result.setUpdated(existingPartitionNames);
        return result;
    }

    protected void addUpdateDropPartitions(final QualifiedName tableQName, final Table table,
        final List<String> partitionNames, final List<PartitionInfo> addedPartitionInfos,
        final List<PartitionHolder> existingPartitionInfos, final Set<String> deletePartitionNames) {
        final String databaseName = table.getDbName();
        final String tableName = table.getTableName();
        final TableInfo tableInfo = hiveMetacatConverters.toTableInfo(tableQName, table);

        try {
            final List<Partition> existingPartitions = existingPartitionInfos.stream()
                .map(p -> hiveMetacatConverters.fromPartitionInfo(tableInfo, p.getPartitionInfo()))
                .collect(Collectors.toList());
            final List<Partition> addedPartitions = addedPartitionInfos.stream()
                .map(p -> hiveMetacatConverters.fromPartitionInfo(tableInfo, p)).collect(Collectors.toList());
            // If alterIfExists=true, then alter partitions if they already exists
            if (!existingPartitionInfos.isEmpty()) {
                copyTableSdToPartitionSd(existingPartitions, table);
                metacatHiveClient.alterPartitions(databaseName,
                    tableName, existingPartitions);
            }

            // Copy the storage details from the table if the partition does not contain the details.
            copyTableSdToPartitionSd(addedPartitions, table);
            // Drop partitions with ids in 'deletePartitionNames' and add 'addedPartitionInfos' partitions
            metacatHiveClient.addDropPartitions(databaseName,
                tableName, addedPartitions, Lists.newArrayList(deletePartitionNames));
        } catch (NoSuchObjectException exception) {
            if (exception.getMessage() != null && exception.getMessage().startsWith("Partition doesn't exist")) {
                throw new PartitionNotFoundException(tableQName, "", exception);
            } else {
                throw new TableNotFoundException(tableQName, exception);
            }
        } catch (MetaException | InvalidObjectException exception) {
            throw new InvalidMetaException("One or more partitions are invalid.", exception);
        } catch (AlreadyExistsException e) {
            throw new PartitionAlreadyExistsException(tableQName, partitionNames, e);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed savePartitions hive table %s", tableName), exception);
        }
    }

    private String getPartitionUri(final PartitionHolder partition) {
        String result = null;
        if (partition.getPartition() != null) {
            final Partition hivePartition = partition.getPartition();
            result = hivePartition.getSd() != null ? hivePartition.getSd().getLocation() : null;
        } else if (partition.getPartitionInfo() != null) {
            final PartitionInfo partitionInfo = partition.getPartitionInfo();
            result = partitionInfo.getSerde() != null ? partitionInfo.getSerde().getUri() : null;
        }
        return result;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void deletePartitions(
        final ConnectorRequestContext requestContext,
        final QualifiedName tableName,
        final List<String> partitionNames
    ) {

        try {
            metacatHiveClient.dropPartitions(tableName.getDatabaseName(), tableName.getTableName(), partitionNames);
        } catch (MetaException | NoSuchObjectException exception) {
            throw new TableNotFoundException(tableName, exception);
        } catch (InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (TException e) {
            //not sure which qualified name to use here
            throw new ConnectorException(String.format("Failed delete partitions for hive table %s", tableName), e);
        }

    }

    /**
     * Returns the list of partition keys.
     *
     * @param fields fields
     * @return partition keys
     */
    protected List<String> getPartitionKeys(final List<FieldSchema> fields) {
        return (fields != null) ? fields.stream().map(FieldSchema::getName).collect(Collectors.toList())
            :   Lists.newArrayList();
    }

    protected Map<String, PartitionHolder> getPartitionsByNames(final Table table, final List<String> partitionNames) {
        final String databasename = table.getDbName();
        final String tablename = table.getTableName();
        try {
            final List<Partition> partitions =
                metacatHiveClient.getPartitions(databasename, tablename, partitionNames);

            return partitions.stream().map(PartitionHolder::new).collect(Collectors.toMap(part -> {
                try {
                    return Warehouse.makePartName(table.getPartitionKeys(), part.getPartition().getValues());
                } catch (Exception e) {
                    throw new InvalidMetaException("One or more partition names are invalid.", e);
                }
            }, Function.identity()));
        } catch (Exception e) {
            throw new InvalidMetaException("One or more partition names are invalid.", e);
        }
    }

    private void copyTableSdToPartitionSd(final List<Partition> hivePartitions, final Table table) {
        //
        // Update the partition info based on that of the table.
        //
        for (Partition partition : hivePartitions) {
            final StorageDescriptor sd = partition.getSd();
            final StorageDescriptor tableSdCopy = table.getSd().deepCopy();
            if (tableSdCopy.getSerdeInfo() == null) {
                final SerDeInfo serDeInfo = new SerDeInfo(null, null, new HashMap<>());
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

    private String getNameOfPartition(final Table table, final Partition partition) {
        try {
            return Warehouse.makePartName(table.getPartitionKeys(), partition.getValues());
        } catch (TException e) {
            throw new InvalidMetaException("One or more partition names are invalid.", e);
        }
    }
}
