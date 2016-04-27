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

package com.netflix.metacat.main.services.impl;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.SavePartitionResult;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePartitionName;
import com.facebook.presto.spi.Sort;
import com.facebook.presto.spi.TableNotFoundException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.HasMetadata;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.monitoring.DynamicGauge;
import com.netflix.metacat.common.monitoring.LogConstants;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.ThreadServiceManager;
import com.netflix.metacat.converters.PrestoConverters;
import com.netflix.metacat.main.presto.split.SplitManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.SessionProvider;
import com.netflix.metacat.main.services.TableService;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.TagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PartitionServiceImpl implements PartitionService {
    private static final Logger log = LoggerFactory.getLogger(PartitionServiceImpl.class);
    @Inject
    CatalogService catalogService;
    @Inject
    PrestoConverters prestoConverters;
    @Inject
    SplitManager splitManager;
    @Inject
    TableService tableService;
    @Inject
    UserMetadataService userMetadataService;
    @Inject
    SessionProvider sessionProvider;
    @Inject
    ThreadServiceManager threadServiceManager;

    private ConnectorPartitionResult getPartitionResult(QualifiedName name, String filter, List<String> partitionNames, Sort sort, Pageable pageable, boolean includePartitionDetails) {
        ConnectorPartitionResult result = null;
        Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (tableHandle.isPresent()) {
            result = splitManager.getPartitions(tableHandle.get(), filter, partitionNames, sort, pageable, includePartitionDetails);
        }
        return result;
    }

    @Override
    public List<PartitionDto> list(QualifiedName name, String filter, List<String> partitionNames, Sort sort
            , Pageable pageable, boolean includeUserDefinitionMetadata, boolean includeUserDataMetadata, boolean includePartitionDetails) {
        ConnectorPartitionResult partitionResult = getPartitionResult(name, filter, partitionNames, sort, pageable, includePartitionDetails);
        List<PartitionDto> result = Collections.emptyList();
        if (partitionResult != null) {
            List<QualifiedName> names = Lists.newArrayList();
            List<String> uris = Lists.newArrayList();
            result = partitionResult.getPartitions().stream()
                    .map(partition -> {
                        PartitionDto result1 = toPartitionDto(name, partition );
                        names.add( result1.getName());
                        uris.add(result1.getDataUri());
                        return result1;
                    })
                    .collect(Collectors.toList());
            if(includeUserDefinitionMetadata || includeUserDataMetadata){
                List<ListenableFuture<Map<String,ObjectNode>>> futures = Lists.newArrayList();
                futures.add(threadServiceManager.getExecutor().submit(() -> includeUserDefinitionMetadata ?
                        userMetadataService.getDefinitionMetadataMap(names) :
                        Maps.newHashMap()));
                futures.add(threadServiceManager.getExecutor().submit(() -> includeUserDataMetadata?
                        userMetadataService.getDataMetadataMap(uris):
                        Maps.newHashMap()));
                try {
                    List<Map<String,ObjectNode>> metadataResults = Futures.successfulAsList(futures).get(1, TimeUnit.HOURS);
                    Map<String,ObjectNode> definitionMetadataMap = metadataResults.get(0);
                    Map<String,ObjectNode> dataMetadataMap = metadataResults.get(1);
                    result.stream().forEach(partitionDto -> userMetadataService.populateMetadata(partitionDto
                            , definitionMetadataMap.get(partitionDto.getName().toString())
                            , dataMetadataMap.get(partitionDto.getDataUri())));
                } catch (Exception e) {
                    Throwables.propagate(e);
                }
            }
        }
        TagList tags = BasicTagList.of("catalog", name.getCatalogName(), "database", name.getDatabaseName(), "table", name.getTableName());
        DynamicGauge.set(LogConstants.GaugeGetPartitionsCount.toString(), tags, result.size());
        log.info("Got {} partitions for {} using filter: {} and partition names: {}", result.size(), name, filter, partitionNames);
        return result;
    }

    @Override
    public Integer count(QualifiedName name) {
        Integer result = 0;
        Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (tableHandle.isPresent()) {
            Session session = sessionProvider.getSession(name);
            result = splitManager.getPartitionCount( session, tableHandle.get());
        }
        return result;
    }

    @Override
    public PartitionsSaveResponseDto save(QualifiedName name, List<PartitionDto> partitionDtos
            , List<String> partitionIdsForDeletes, boolean checkIfExists, boolean alterIfExists) {
        PartitionsSaveResponseDto result = new PartitionsSaveResponseDto();
        // If no partitions are passed, then return
        if( partitionDtos == null || partitionDtos.isEmpty()){
            return result;
        }
        TagList tags = BasicTagList.of("catalog", name.getCatalogName(), "database", name.getDatabaseName(), "table",
                name.getTableName());
        DynamicGauge.set(LogConstants.GaugeAddPartitions.toString(), tags, partitionDtos.size());
        Session session = sessionProvider.getSession(name);
        TableHandle tableHandle = tableService.getTableHandle(name).orElseThrow(() ->
                new MetacatNotFoundException("Unable to locate " + name));
        List<ConnectorPartition> partitions = partitionDtos.stream()
                .map(prestoConverters::fromPartitionDto)
                .collect(Collectors.toList());
        List<HasMetadata> deletePartitions = Lists.newArrayList();
        if( partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            DynamicGauge.set(LogConstants.GaugeDeletePartitions.toString(), tags, partitionIdsForDeletes.size());
            ConnectorPartitionResult deletePartitionResult = splitManager.getPartitions(tableHandle, null, partitionIdsForDeletes, null, null, false);
            deletePartitions = deletePartitionResult.getPartitions().stream()
                    .map(partition -> toPartitionDto(name, partition ))
                    .collect(Collectors.toList());
        }
        //
        // Save all the new and updated partitions
        //
        log.info("Saving partitions({}) for {}", partitions.size(), name);
        SavePartitionResult savePartitionResult = splitManager.savePartitions(tableHandle, partitions, partitionIdsForDeletes,
                checkIfExists, alterIfExists);

        // Save metadata
        log.info("Saving user metadata for partitions for {}", name);
        // delete metadata
        if( !deletePartitions.isEmpty()) {
            log.info("Deleting user metadata for partitions with names {} for {}", partitionIdsForDeletes, name);
            userMetadataService.deleteMetadatas(deletePartitions, false);
        }
        userMetadataService.saveMetadatas(session.getUser(), partitionDtos, true);

        result.setUpdated(savePartitionResult.getUpdated());
        result.setAdded(savePartitionResult.getAdded());

        return result;
    }

    @Override
    public void delete(QualifiedName name, List<String> partitionIds) {
        TagList tags = BasicTagList.of("catalog", name.getCatalogName(), "database", name.getDatabaseName(), "table", name.getTableName());
        DynamicGauge.set(LogConstants.GaugeDeletePartitions.toString(), tags, partitionIds.size());
        Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if( !tableHandle.isPresent()){
            throw new TableNotFoundException(new SchemaTableName(name.getDatabaseName(), name.getTableName()));
        }
        if (!partitionIds.isEmpty()) {
            ConnectorPartitionResult partitionResult = splitManager.getPartitions(tableHandle.get(), null, partitionIds, null, null, false);
            log.info("Deleting partitions with names {} for {}", partitionIds, name);
            splitManager.deletePartitions( tableHandle.get(), partitionIds);
            List<HasMetadata> partitions = partitionResult.getPartitions().stream()
                    .map(partition -> toPartitionDto(name, partition ))
                    .collect(Collectors.toList());
            // delete metadata
            log.info("Deleting user metadata for partitions with names {} for {}", partitionIds, name);
            userMetadataService.deleteMetadatas(partitions, false);
        }
    }

    @Override
    public List<QualifiedName> getQualifiedNames(String uri, boolean prefixSearch){
        List<QualifiedName> result = Lists.newCopyOnWriteArrayList();
        List<ListenableFuture<Void>> futures = Lists.newArrayList();
        catalogService.getCatalogNames().stream().forEach(catalog -> {
            Session session = sessionProvider.getSession(QualifiedName.ofCatalog(catalog.getCatalogName()));
            futures.add(threadServiceManager.getExecutor().submit(() -> {
                List<SchemaTablePartitionName> schemaTablePartitionNames = splitManager
                        .getPartitionNames(session, uri, prefixSearch);
                List<QualifiedName> qualifiedNames = schemaTablePartitionNames.stream().map(
                        schemaTablePartitionName -> QualifiedName.ofPartition(catalog.getConnectorName()
                                , schemaTablePartitionName.getTableName().getSchemaName()
                                , schemaTablePartitionName.getTableName().getTableName()
                                , schemaTablePartitionName.getPartitionId())).collect(Collectors.toList());
                result.addAll(qualifiedNames);
                return null;
            }));
        });
        try {
            Futures.allAsList(futures).get(1, TimeUnit.HOURS);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
        return result;
    }

    @Override
    public List<String> getPartitionKeys(QualifiedName name, String filter, List<String> partitionNames, Sort sort,
            Pageable pageable) {
        List<String> result = Lists.newArrayList();
        Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (tableHandle.isPresent()) {
            result = splitManager.getPartitionKeys(tableHandle.get(), filter, partitionNames, sort, pageable);
        }
        return result;
    }

    @Override
    public List<String> getPartitionUris(QualifiedName name, String filter, List<String> partitionNames, Sort sort,
            Pageable pageable) {
        List<String> result = Lists.newArrayList();
        Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (tableHandle.isPresent()) {
            result = splitManager.getPartitionUris(tableHandle.get(), filter, partitionNames, sort, pageable);
        }
        return result;
    }

    @Override
    public void create( @Nonnull QualifiedName name, @Nonnull PartitionDto dto) {
        save( name, Lists.newArrayList(dto), null, false, false);
    }

    @Override
    public void update(@Nonnull QualifiedName name, @Nonnull PartitionDto dto) {
        save( name, Lists.newArrayList(dto), null, true, false);
    }

    @Override
    public void delete(@Nonnull QualifiedName name) {
        QualifiedName tableName = QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
        delete( tableName, Lists.newArrayList(name.getPartitionName()));
    }

    @Override
    public PartitionDto get(@Nonnull QualifiedName name) {
        PartitionDto result = null;
        QualifiedName tableName = QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
        List<PartitionDto> dtos = list( tableName, null, Lists.newArrayList(name.getPartitionName()), null, null, true, true, true);
        if( !dtos.isEmpty()){
            result = dtos.get(0);
        }
        return result;
    }

    @Override
    public boolean exists(@Nonnull QualifiedName name) {
        return get(name)!=null;
    }

    private PartitionDto toPartitionDto(QualifiedName tableName, ConnectorPartition partition) {
        QualifiedName partitionName = QualifiedName.ofPartition(
                tableName.getCatalogName(),
                tableName.getDatabaseName(),
                tableName.getTableName(),
                partition.getPartitionId()
        );
        return prestoConverters.toPartitionDto(partitionName, partition);
    }
}
