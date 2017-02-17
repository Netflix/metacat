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
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveResponse;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.exception.InvalidMetaException;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;

/**
 * HiveConnectorPartitionService.
 *
 * @author zhenl
 */
public class HiveConnectorPartitionService implements ConnectorPartitionService {
    private final MetacatHiveClient metacatHiveClient;
    private final HiveConnectorInfoConverter hiveMetacatConverters;
    private final String catalogName;

    /**
     * Constructor.
     * @param catalogName catalogname
     * @param metacatHiveClient     hive client
     * @param hiveMetacatConverters hive converter
     */
    @Inject
    public HiveConnectorPartitionService(@Named("catalogName") final String catalogName,
                                         @Nonnull final MetacatHiveClient metacatHiveClient,
                                         @Nonnull final HiveConnectorInfoConverter hiveMetacatConverters) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
        this.catalogName = catalogName;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<PartitionInfo> getPartitions(
        @Nonnull final ConnectorContext requestContext,
        @Nonnull final QualifiedName tableName,
        @Nonnull final PartitionListRequest partitionsRequest
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
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + tableName, e);
        } catch (TException e) {
            throw new TableNotFoundException(tableName, e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int getPartitionCount(
        @Nonnull final ConnectorContext requestContext,
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
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionKeys(
        @Nonnull final ConnectorContext requestContext,
        @Nonnull final QualifiedName tableName,
        @Nonnull final PartitionListRequest partitionsRequest
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
            } else {
                if (partitionIds != null) {
                    partitionList = metacatHiveClient.listPartitions(tableName.getDatabaseName(),
                        tableName.getTableName(), partitionIds);
                }
                if (partitionList == null || partitionList.isEmpty()) {
                    partitionList = metacatHiveClient.listAllPartitions(tableName.getDatabaseName(),
                        tableName.getTableName());
                }
            }
            final List<Partition> filteredPartitionList = Lists.newArrayList();
            partitionList.forEach(partition -> {
                final String partitionName = getNameOfPartition(table, partition);
                if (partitionIds == null || partitionIds.contains(partitionName)) {
                    filteredPartitionList.add(partition);
                }
            });
            partitions = filteredPartitionList;
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
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionUris(
        @Nonnull final ConnectorContext requestContext,
        @Nonnull final QualifiedName table,
        @Nonnull final PartitionListRequest partitionsRequest
    ) {
        final List<String> uris = Lists.newArrayList();
        for (Partition partition : getPartitions(table, partitionsRequest.getFilter(),
            partitionsRequest.getPartitionNames(), partitionsRequest.getSort(), partitionsRequest.getPageable())) {
            uris.add(partition.getSd().getLocation());
        }
        return uris;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public PartitionsSaveResponse savePartitions(
        @Nonnull final ConnectorContext requestContext,
        @Nonnull final QualifiedName tableName,
        @Nonnull final PartitionsSaveRequest partitionsSaveRequest
    ) {

        try {
            final Table table = metacatHiveClient.getTableByName(tableName.getDatabaseName(), tableName.getTableName());
            final List<PartitionInfo> partitionInfos = partitionsSaveRequest.getPartitions();
            final List<Partition> partitions = Lists.newArrayList();
            final TableInfo tableInfo = hiveMetacatConverters.toTableInfo(tableName, table);
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(hiveMetacatConverters.fromPartitionInfo(tableInfo, partitionInfo));
            }
            metacatHiveClient.savePartitions(partitions);
            final List<String> partitionNames = Lists.newArrayList();
            for (Partition partition : partitions) {
                partitionNames.add(getNameOfPartition(table, partition));
            }
            final PartitionsSaveResponse partitionsSaveResponse = new PartitionsSaveResponse();
            partitionsSaveResponse.setAdded(partitionNames);
            return partitionsSaveResponse;
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (TException e) {
            throw new TableNotFoundException(tableName, e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void deletePartitions(
        @Nonnull final ConnectorContext requestContext,
        @Nonnull final QualifiedName tableName,
        @Nonnull final List<String> partitionNames
    ) {

        try {
            metacatHiveClient.dropPartition(tableName.getDatabaseName(), tableName.getTableName(), partitionNames);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (TException e) {
            //not sure which qualified name to use here
            throw new TableNotFoundException(tableName, e);
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
