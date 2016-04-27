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

package com.netflix.metacat.s3.connector;

import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.spi.AuditInfo;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionDetail;
import com.facebook.presto.spi.ConnectorPartitionDetailImpl;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitDetailManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.SavePartitionResult;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePartitionName;
import com.facebook.presto.spi.Sort;
import com.facebook.presto.spi.StorageInfo;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.persist.Transactional;
import com.netflix.metacat.common.partition.parser.PartitionParser;
import com.netflix.metacat.common.partition.util.FilterPartition;
import com.netflix.metacat.common.partition.util.PartitionUtil;
import com.netflix.metacat.common.partition.visitor.PartitionKeyParserEval;
import com.netflix.metacat.common.partition.visitor.PartitionParamParserEval;
import com.netflix.metacat.s3.connector.dao.PartitionDao;
import com.netflix.metacat.s3.connector.dao.TableDao;
import com.netflix.metacat.s3.connector.model.Info;
import com.netflix.metacat.s3.connector.model.Location;
import com.netflix.metacat.s3.connector.model.Partition;
import com.netflix.metacat.s3.connector.model.Table;
import com.netflix.metacat.s3.connector.util.ConverterUtil;

import javax.inject.Inject;
import java.io.StringReader;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveUtil.schemaTableName;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by amajumdar on 10/9/15.
 */
@Transactional
public class S3SplitDetailManager implements ConnectorSplitDetailManager{
    @Inject
    TableDao tableDao;
    @Inject
    PartitionDao partitionDao;
    @Inject
    HiveConnectorId connectorId;
    @Inject
    ConverterUtil converterUtil;
    private static final String FIELD_DATE_CREATED = "dateCreated";
    private static final String FIELD_BATCHID = "batchid";

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle table, String filterExpression,
            List<String> partitionIds, Sort sort, Pageable pageable, boolean includePartitionDetails) {
        SchemaTableName tableName = schemaTableName(table);
        return new ConnectorPartitionResult( _getPartitions(tableName, filterExpression, partitionIds, sort, pageable, includePartitionDetails)
                , TupleDomain.<ColumnHandle>none());
    }

    private List<ConnectorPartition> _getPartitions(SchemaTableName tableName, final String filterExpression
            , List<String> partitionIds, Sort sort, Pageable pageable, boolean includePartitionDetails) {
        //
        // Limiting the in clause to 5000 part names because the sql query with the IN clause for part_name(767 bytes)
        // will hit the max sql query length(max_allowed_packet for our RDS) if we use more than 5400 or so
        //
        final List<ConnectorPartition> partitions = Lists.newArrayList();
        if( partitionIds != null && partitionIds.size() > 5000){
            List<List<String>> subFilterPartitionNamesList = Lists.partition( partitionIds, 5000);
            subFilterPartitionNamesList.forEach(
                    subPartitionIds -> partitions.addAll( _getConnectorPartitions(tableName, filterExpression,
                            subPartitionIds, sort, pageable, includePartitionDetails)));
        } else {
            partitions.addAll(_getConnectorPartitions(tableName, filterExpression, partitionIds, sort, pageable, includePartitionDetails));
        }
        return partitions;
    }

    private List<ConnectorPartition> _getConnectorPartitions(SchemaTableName tableName, final String filterExpression
            , List<String> partitionIds, Sort sort, Pageable pageable, boolean includePartitionDetails) {
        // batch exists
        boolean isBatched = !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_BATCHID);
        // Support for dateCreated
        boolean hasDateCreated = !Strings.isNullOrEmpty(filterExpression) && filterExpression.contains(FIELD_DATE_CREATED);
        String dateCreatedSqlCriteria = null;
        if( hasDateCreated){
            dateCreatedSqlCriteria = getDateCreatedSqlCriteria( filterExpression);
        }
        // Table
        Table table = tableDao.getBySourceDatabaseTableName(connectorId.toString(), tableName.getSchemaName(), tableName.getTableName());
        if ( table == null) {
            throw new TableNotFoundException(tableName);
        }
        Collection<String> singlePartitionExprs = getSinglePartitionExprs( filterExpression);
        List<Partition> partitions = partitionDao.getPartitions(table.getId(), partitionIds, singlePartitionExprs, dateCreatedSqlCriteria, sort, Strings.isNullOrEmpty(filterExpression)?pageable:null);
        FilterPartition filter = new FilterPartition();
        //
        Map<ColumnHandle, Comparable<?>> domainMap = Maps.newHashMapWithExpectedSize(1);
        domainMap.put(new ColumnHandle(){}, "ignore");
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withFixedValues(domainMap);

        List<ConnectorPartition> result = partitions.stream().filter(partition -> {
            Map<String, String> values = null;
            if( hasDateCreated){
                values = Maps.newHashMap();
                values.put(FIELD_DATE_CREATED, (partition.getCreatedDate().getTime() / 1000) + "");
            }
            return Strings.isNullOrEmpty(filterExpression)
                    || filter.evaluatePartitionExpression( filterExpression, partition.getName(), partition.getUri(), isBatched, values);
        }).map( partition -> {
            StorageInfo storageInfo = new StorageInfo();
            Location location = table.getLocation();
            if( location != null){
                Info info = location.getInfo();
                if( info != null){
                    storageInfo.setInputFormat(info.getInputFormat());
                    storageInfo.setOutputFormat(info.getOutputFormat());
                    storageInfo.setSerializationLib(info.getSerializationLib());
                    if( includePartitionDetails){
                        storageInfo.setParameters(Maps.newHashMap(info.getParameters()));
                    }
                }
            }
            storageInfo.setUri(partition.getUri());
            AuditInfo auditInfo = new AuditInfo();
            Date createdDate = partition.getCreatedDate();
            if( createdDate != null) {
                auditInfo.setCreatedDate( createdDate.getTime()/1000);
            }
            Date lastUpdatedDate = partition.getLastUpdatedDate();
            if( lastUpdatedDate != null) {
                auditInfo.setLastUpdatedDate( lastUpdatedDate.getTime()/1000);
            }
            return new ConnectorPartitionDetailImpl(partition.getName(), tupleDomain, storageInfo, null, auditInfo);
        }).collect(Collectors.toList());
        //
        if( pageable != null && pageable.isPageable() && !Strings.isNullOrEmpty(filterExpression)){
            int limit = pageable.getLimit();
            if( result.size() < limit){
                limit = result.size();
            }
            result = result.subList(pageable.getOffset(), limit);
        }
        return result;
    }

    private String getDateCreatedSqlCriteria(String filterExpression) {
        StringBuilder result = new StringBuilder();
        Collection<String> values = Lists.newArrayList();
        if( !Strings.isNullOrEmpty(filterExpression)) {
            try {
                values = (Collection<String>) new PartitionParser(new StringReader(filterExpression)).filter()
                        .jjtAccept(new PartitionParamParserEval(), null);
            } catch (Throwable ignored) {
                //
            }
        }
        for(String value : values){
            if( result.length() != 0){
                result.append(" and ");
            }
            result.append(value.replace("dateCreated", "to_seconds(p.date_created)"));
        }
        return result.toString();
    }

    private Collection<String> getSinglePartitionExprs(String filterExpression) {
        Collection<String> result = Lists.newArrayList();
        if( !Strings.isNullOrEmpty(filterExpression)) {
            try {
                result = (Collection<String>) new PartitionParser(new StringReader(filterExpression)).filter()
                        .jjtAccept(new PartitionKeyParserEval(), null);
            } catch (Throwable ignored) {
                //
            }
        }
        if( result != null) {
            result = result.stream().filter(s -> !(s.startsWith("batchid=") || s.startsWith("dateCreated="))).collect(
                    Collectors.toList());
        }
        return result;
    }

    @Override
    @Transactional
    public SavePartitionResult savePartitions(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions
            , List<String> partitionIdsForDeletes, boolean checkIfExists, boolean alterIfExists) {
        checkNotNull(tableHandle, "tableHandle is null");
        SavePartitionResult result = new SavePartitionResult();
        SchemaTableName tableName = schemaTableName(tableHandle);
        // Table
        Table table = tableDao.getBySourceDatabaseTableName(connectorId.toString(), tableName.getSchemaName(),
                tableName.getTableName());
        if ( table == null) {
            throw new TableNotFoundException(tableName);
        }

        // New partition ids
        List<String> addedPartitionIds = Lists.newArrayList();
        // Updated partition ids
        List<String> existingPartitionIds = Lists.newArrayList();
        //
        Map<String,Partition> existingPartitionMap = Maps.newHashMap();

        if( checkIfExists) {
            List<String> partitionNames = partitions.stream().map(
                    partition -> {
                        String partitionName = partition.getPartitionId();
                        PartitionUtil.validatePartitionName(partitionName, converterUtil.partitionKeys(table));
                        return partitionName;
                    }).collect(Collectors.toList());
            existingPartitionMap = getPartitionsByNames(table.getId(), partitionNames);
        }

        // New partitions
        List<Partition> s3Partitions = Lists.newArrayList();
        for(ConnectorPartition partition:partitions){
            String partitionName = partition.getPartitionId();
            Partition s3Partition = existingPartitionMap.get(partitionName);
            if(s3Partition == null){
                addedPartitionIds.add(partitionName);
                s3Partitions.add(converterUtil.toPartition(table, partition));
            } else {
                ConnectorPartitionDetail partitionDetail = (ConnectorPartitionDetail) partition;
                String partitionUri = converterUtil.getUri(partitionDetail);
                String s3PartitionUri = s3Partition.getUri();
                if( partitionUri != null && !partitionUri.equals( s3PartitionUri)){
                    s3Partition.setUri(partitionUri);
                    existingPartitionIds.add(partitionName);
                    s3Partitions.add(s3Partition);
                }
            }
        }
        if( partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            partitionDao.deleteByNames(connectorId.toString(), tableName.getSchemaName(),
                    tableName.getTableName(), partitionIdsForDeletes);
        }
        partitionDao.save(s3Partitions);

        result.setAdded( addedPartitionIds);
        result.setUpdated( existingPartitionIds);
        return result;
    }

    private Map<String, Partition> getPartitionsByNames(Long tableId,
            List<String> partitionNames) {
        List<Partition> partitions = partitionDao.getPartitions( tableId, partitionNames, null, null, null, null);
        return partitions.stream().collect(Collectors.toMap(Partition::getName, partition -> partition));
    }

    @Override
    @Transactional
    public void deletePartitions(ConnectorTableHandle tableHandle, List<String> partitionIds) {
        SchemaTableName schemaTableName = schemaTableName(tableHandle);
        partitionDao.deleteByNames(connectorId.toString(), schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionIds);
    }

    @Override
    public Integer getPartitionCount(ConnectorTableHandle tableHandle) {
        SchemaTableName schemaTableName = schemaTableName(tableHandle);
        return partitionDao.count(connectorId.toString(), schemaTableName.getSchemaName(),
                schemaTableName.getTableName());
    }

    @Override
    public List<SchemaTablePartitionName> getPartitionNames(String uri, boolean prefixSearch) {
        List<Partition> partitions = partitionDao.getByUri( uri, prefixSearch);
        return partitions.stream().map(partition ->
                new SchemaTablePartitionName(
                        new SchemaTableName(partition.getTable().getDatabase().getName(), partition.getTable().getName()), partition.getName()))
                .collect(Collectors.toList());
    }


    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, ConnectorTableLayoutHandle layout) {
        return null;
    }

    public List<String> getPartitionKeys(ConnectorTableHandle tableHandle, String filterExpression, List<String> partitionNames, Sort sort, Pageable pageable){
        SchemaTableName tableName = schemaTableName(tableHandle);
        return _getPartitions( tableName, filterExpression, partitionNames, sort, pageable, true).stream().map(
                ConnectorPartition::getPartitionId).collect(Collectors.toList());
    }

    public List<String> getPartitionUris(ConnectorTableHandle tableHandle, String filterExpression, List<String> partitionNames, Sort sort, Pageable pageable){
        SchemaTableName tableName = schemaTableName(tableHandle);
        return _getPartitions(tableName, filterExpression, partitionNames, sort, pageable, true).stream().map(
                partition -> ((ConnectorPartitionDetail) partition).getStorageInfo().getUri()).collect(Collectors.toList());
    }
}
