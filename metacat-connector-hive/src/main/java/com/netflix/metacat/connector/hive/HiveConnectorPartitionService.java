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
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.exception.InvalidMetaException;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.connector.hive.converters.HiveMetacatConverters;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HiveConnectorPartitionService.
 *
 * @author zhenl
 */
public class HiveConnectorPartitionService implements ConnectorPartitionService {
    private final MetacatHiveClient metacatHiveClient;


    private final HiveMetacatConverters hiveMetacatConverters;

    /**
     * Constructor.
     *
     * @param metacatHiveClient     hive client
     * @param hiveMetacatConverters hive converter
     */
    @Inject
    public HiveConnectorPartitionService(@Nonnull final MetacatHiveClient metacatHiveClient,
                                         @Nonnull final HiveMetacatConverters hiveMetacatConverters) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
    }

    /**
     * Gets the Partitions based on a filter expression for the specified table.
     *
     * @param requestContext    The Metacat request context
     * @param name              name
     * @param partitionsRequest The metadata for what kind of partitions to get from the table
     * @param sort              sort by and order
     * @param pageable          pagination info
     * @return filtered list of partitions
     */
    public List<PartitionDto> getPartitions(
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final QualifiedName name,
        @Nonnull final GetPartitionsRequestDto partitionsRequest,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        try {
            final List<Partition> partitions = getPartitions(name,
                partitionsRequest.getFilter(), partitionsRequest.getPartitionNames(), sort, pageable);
            final Table table = metacatHiveClient.getTableByName(name.getDatabaseName(), name.getTableName());

            final TableDto tableDto = hiveMetacatConverters.hiveToMetacatTable(name, table);
            final List<PartitionDto> partitionDtos = new ArrayList<>();
            for (Partition partition : partitions) {
                partitionDtos.add(hiveMetacatConverters.hiveToMetacatPartition(tableDto, partition));
            }
            return partitionDtos;
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + name, e);
        } catch (TException e) {
            throw new TableNotFoundException(name, e);
        }
    }

    /**
     * Number of partitions for the given table.
     *
     * @param requestContext The Metacat request context
     * @param tableName      table handle
     * @return Number of partitions
     */
    public int getPartitionCount(
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final QualifiedName tableName
    ) {
        try {
            return metacatHiveClient.getPartitionCount(tableName.getDatabaseName(), tableName.getTableName());
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + tableName, e);
        } catch (TException e) {
            throw new TableNotFoundException(tableName, e);
        }
    }

    /**
     * Gets the partition names/keys based on a filter expression for the specified table.
     *
     * @param requestContext    The Metacat request context
     * @param tableName         table handle to get partition for
     * @param partitionsRequest The metadata for what kind of partitions to get from the table
     * @param sort              sort by and order
     * @param pageable          pagination info
     * @return filtered list of partition names
     */
    public List<String> getPartitionKeys(
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final QualifiedName tableName,
        @Nonnull final GetPartitionsRequestDto partitionsRequest,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {

        final String filterExpression = partitionsRequest.getFilter();
        final List<String> partitionIds = partitionsRequest.getPartitionNames();
        List<String> names = Lists.newArrayList();

        try {
            if (filterExpression != null || (partitionIds != null && !partitionIds.isEmpty())) {
                final Table table = metacatHiveClient.getTableByName(tableName.getDatabaseName(),
                    tableName.getTableName());
                for (Partition partition : getPartitions(tableName, filterExpression, partitionIds, sort, pageable)) {
                    names.add(getNameOfPartition(table, partition));
                }
            } else {
                names = metacatHiveClient.getPartitionNames(tableName.getDatabaseName(), tableName.getTableName());
                if (null != pageable && pageable.isPageable()) {
                    final int limit = Math.min(pageable.getOffset() + pageable.getLimit(), names.size());
                    return (pageable.getOffset() > limit) ? Lists.newArrayList()
                        : names.subList(pageable.getOffset(), limit);
                }
            }
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + tableName, e);
        } catch (TException e) {
            throw new TableNotFoundException(tableName, e);
        }
        return names;
    }

    private List<Partition> getPartitions(@Nonnull final QualifiedName tableName,
                                          @Nullable final String filter,
                                          @Nullable final List<String> partitionIds,
                                          @Nullable final Sort sort,
                                          @Nullable final Pageable pageable) {
        List<Partition> partitions = null;
        try {
            final Table table = metacatHiveClient.getTableByName(tableName.getDatabaseName(), tableName.getTableName());
            List<Partition> partitionList = null;
            if (!Strings.isNullOrEmpty(filter)) {
                partitionList = metacatHiveClient.listPartitionsByFilter(tableName.getDatabaseName(),
                    tableName.getTableName(), filter);
                final List<Partition> filteredPartitionList = Lists.newArrayList();
                partitionList.forEach(partition -> {
                    final String partitionName = getNameOfPartition(table, partition);
                    if (partitionIds == null || partitionIds.contains(partitionName)) {
                        filteredPartitionList.add(partition);
                    }
                });
                partitions = partitionList;

            } else if (partitionIds != null && !partitionIds.isEmpty()) {
                partitions = metacatHiveClient.listPartitions(tableName.getDatabaseName(),
                    tableName.getTableName(), partitionIds);
            } else {
                partitions = metacatHiveClient.listAllPartitions(tableName.getDatabaseName(), tableName.getTableName());
            }

            if (null != pageable && pageable.isPageable()) {
                final int limit = Math.min(pageable.getOffset() + pageable.getLimit(), partitions.size());
                partitions = (pageable.getOffset() > limit) ? Lists.newArrayList()
                    : partitions.subList(pageable.getOffset(), limit);
            }
            return partitions;
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + tableName, e);
        } catch (TException e) {
            throw new TableNotFoundException(tableName, e);
        }
    }

    /**
     * Gets the partition uris based on a filter expression for the specified table.
     *
     * @param requestContext    The Metacat request context
     * @param table             table handle to get partition for
     * @param partitionsRequest The metadata for what kind of partitions to get from the table
     * @param sort              sort by and order
     * @param pageable          pagination info
     * @return filtered list of partition uris
     */
    public List<String> getPartitionUris(
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final QualifiedName table,
        @Nonnull final GetPartitionsRequestDto partitionsRequest,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable
    ) {
        final List<String> uris = Lists.newArrayList();
        for (Partition partition : getPartitions(table, partitionsRequest.getFilter(),
            partitionsRequest.getPartitionNames(), sort, pageable)) {
            uris.add(partition.getSd().getLocation());
        }
        return uris;
    }

    /**
     * Add/Update/delete partitions for a table.
     *
     * @param requestContext        The Metacat request context
     * @param tableName             table handle to get partition for
     * @param partitionsSaveRequest Partitions to save, alter or delete
     * @return added/updated list of partition names
     */
    public PartitionsSaveResponseDto savePartitions(
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final QualifiedName tableName,
        @Nonnull final PartitionsSaveRequestDto partitionsSaveRequest
    ) {

        try {
            final Table table = metacatHiveClient.getTableByName(tableName.getDatabaseName(), tableName.getTableName());
            final List<PartitionDto> partitionDtos = partitionsSaveRequest.getPartitions();
            final List<Partition> partitions = Lists.newArrayList();
            final PartitionsSaveResponseDto partitionsSaveResponseDto = new PartitionsSaveResponseDto();
            final TableDto tableDto = hiveMetacatConverters.hiveToMetacatTable(tableName, table);
            for (PartitionDto partitionDto : partitionDtos) {
                partitions.add(hiveMetacatConverters.metacatToHivePartition(partitionDto, tableDto));
            }
            metacatHiveClient.savePartitions(partitions);
            final List<String> partitionNames = Lists.newArrayList();
            for (Partition partition : partitions) {
                partitionNames.add(getNameOfPartition(table, partition));
            }
            partitionsSaveResponseDto.setAdded(partitionNames);
            return partitionsSaveResponseDto;
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (TException e) {
            throw new TableNotFoundException(tableName, e);
        }
    }

    /**
     * Delete partitions for a table.
     *
     * @param requestContext The Metacat request context
     * @param partitions     list of partition to delete
     */
    public void deletePartitions(
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final List<QualifiedName> partitions
    ) {
        final Map<String, Map<String, List<String>>> partsMap = new HashMap<>();
        String catalogName = "";
        for (final QualifiedName qualifiedName : partitions) {
            final String databaseName = qualifiedName.getDatabaseName();
            catalogName = qualifiedName.getCatalogName();
            if (!partsMap.containsKey(databaseName)) {
                partsMap.put(databaseName, new HashMap<>());
            }
            final String tableName = qualifiedName.getTableName();
            if (!partsMap.get(databaseName).containsKey(tableName)) {
                partsMap.get(databaseName).put(tableName, new ArrayList<>());
            }
            partsMap.get(databaseName).get(tableName).add(qualifiedName.getPartitionName());
        }
        for (Map.Entry<String, Map<String, List<String>>> database : partsMap.entrySet()) {
            for (Map.Entry<String, List<String>> tableEntry : database.getValue().entrySet()) {
                try {
                    metacatHiveClient.dropPartition(database.getKey(), tableEntry.getKey(), tableEntry.getValue());
                } catch (MetaException | InvalidObjectException e) {
                    throw new InvalidMetaException("One or more partitions are invalid.", e);
                } catch (TException e) {
                    //not sure which qualified name to use here
                    throw new TableNotFoundException(
                        QualifiedName.ofTable(catalogName, database.getKey(), tableEntry.getKey()), e);
                }
            }
        }

    }

    /**
     * get name of partitions.
     *
     * @param table
     * @param partition
     * @return partition name
     */
    String getNameOfPartition(@Nonnull final Table table, @Nonnull final Partition partition) {
        try {
            return Warehouse.makePartName(table.getPartitionKeys(), partition.getValues());
        } catch (Exception e) {
            throw new InvalidMetaException("One or more partition names are invalid.", e);
        }
    }
}
