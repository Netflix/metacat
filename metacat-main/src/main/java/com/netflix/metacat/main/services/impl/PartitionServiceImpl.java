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
import com.google.common.base.Strings;
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
import com.netflix.metacat.common.server.Config;
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
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Partition service.
 */
@Slf4j
public class PartitionServiceImpl implements PartitionService {
    @Inject
    private CatalogService catalogService;
    @Inject
    private PrestoConverters prestoConverters;
    @Inject
    private SplitManager splitManager;
    @Inject
    private TableService tableService;
    @Inject
    private UserMetadataService userMetadataService;
    @Inject
    private SessionProvider sessionProvider;
    @Inject
    private ThreadServiceManager threadServiceManager;
    @Inject
    private Config config;

    private ConnectorPartitionResult getPartitionResult(final QualifiedName name, final String filter,
        final List<String> partitionNames,
        final Sort sort, final Pageable pageable, final boolean includePartitionDetails) {
        ConnectorPartitionResult result = null;
        final Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (tableHandle.isPresent()) {
            result = splitManager
                .getPartitions(tableHandle.get(), filter, partitionNames, sort, pageable, includePartitionDetails);
        }
        return result;
    }

    @Override
    public List<PartitionDto> list(final QualifiedName name, final String filter, final List<String> partitionNames,
        final Sort sort, final Pageable pageable,
        final boolean includeUserDefinitionMetadata, final boolean includeUserDataMetadata,
        final boolean includePartitionDetails) {
        if (Strings.isNullOrEmpty(filter)
            && (pageable == null || !pageable.isPageable())
            && (partitionNames == null || partitionNames.isEmpty())
            && config.getQualifiedNamesToThrowErrorWhenNoFilterOnListPartitions().contains(name)) {
            throw new IllegalArgumentException(String.format("No filter or limit specified for table %s", name));
        }
        final ConnectorPartitionResult partitionResult =
            getPartitionResult(name, filter, partitionNames, sort, pageable,
            includePartitionDetails);
        List<PartitionDto> result = Collections.emptyList();
        if (partitionResult != null) {
            final List<QualifiedName> names = Lists.newArrayList();
            final List<String> uris = Lists.newArrayList();
            result = partitionResult.getPartitions().stream()
                .map(partition -> {
                    final PartitionDto result1 = toPartitionDto(name, partition);
                    names.add(result1.getName());
                    uris.add(result1.getDataUri());
                    return result1;
                })
                .collect(Collectors.toList());
            final TagList tags = BasicTagList
                .of("catalog", name.getCatalogName(),
                    "database", name.getDatabaseName(), "table", name.getTableName());
            DynamicGauge.set(LogConstants.GaugeGetPartitionsCount.toString(), tags, result.size());
            log.info("Got {} partitions for {} using filter: {} and partition names: {}",
                result.size(), name, filter,
                partitionNames);
            if (includeUserDefinitionMetadata || includeUserDataMetadata) {
                final List<ListenableFuture<Map<String, ObjectNode>>> futures = Lists.newArrayList();
                futures.add(threadServiceManager.getExecutor().submit(() -> includeUserDefinitionMetadata
                    ? userMetadataService.getDefinitionMetadataMap(names)
                    : Maps.newHashMap()));
                futures.add(threadServiceManager.getExecutor().submit(() -> includeUserDataMetadata
                    ? userMetadataService.getDataMetadataMap(uris)
                    : Maps.newHashMap()));
                try {
                    final List<Map<String, ObjectNode>> metadataResults = Futures.successfulAsList(futures)
                        .get(1, TimeUnit.HOURS);
                    final Map<String, ObjectNode> definitionMetadataMap = metadataResults.get(0);
                    final Map<String, ObjectNode> dataMetadataMap = metadataResults.get(1);
                    result.forEach(partitionDto -> userMetadataService.populateMetadata(partitionDto,
                        definitionMetadataMap.get(partitionDto.getName().toString()),
                        dataMetadataMap.get(partitionDto.getDataUri())));
                } catch (Exception e) {
                    Throwables.propagate(e);
                }
            }
        }
        return result;
    }

    @Override
    public Integer count(final QualifiedName name) {
        Integer result = 0;
        final Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (tableHandle.isPresent()) {
            final Session session = sessionProvider.getSession(name);
            result = splitManager.getPartitionCount(session, tableHandle.get());
        }
        return result;
    }

    @Override
    public PartitionsSaveResponseDto save(final QualifiedName name, final List<PartitionDto> partitionDtos,
        final List<String> partitionIdsForDeletes, final boolean checkIfExists, final boolean alterIfExists) {
        final PartitionsSaveResponseDto result = new PartitionsSaveResponseDto();
        // If no partitions are passed, then return
        if (partitionDtos == null || partitionDtos.isEmpty()) {
            return result;
        }
        final TagList tags = BasicTagList.of("catalog", name.getCatalogName(),
            "database", name.getDatabaseName(), "table",
            name.getTableName());
        DynamicGauge.set(LogConstants.GaugeAddPartitions.toString(), tags, partitionDtos.size());
        final Session session = sessionProvider.getSession(name);
        final TableHandle tableHandle = tableService.getTableHandle(name).orElseThrow(() ->
            new MetacatNotFoundException("Unable to locate " + name));
        final List<ConnectorPartition> partitions = partitionDtos.stream()
            .map(prestoConverters::fromPartitionDto)
            .collect(Collectors.toList());
        List<HasMetadata> deletePartitions = Lists.newArrayList();
        if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            DynamicGauge.set(LogConstants.GaugeDeletePartitions.toString(), tags, partitionIdsForDeletes.size());
            final ConnectorPartitionResult deletePartitionResult = splitManager
                .getPartitions(tableHandle, null, partitionIdsForDeletes, null, null, false);
            deletePartitions = deletePartitionResult.getPartitions().stream()
                .map(partition -> toPartitionDto(name, partition))
                .collect(Collectors.toList());
        }
        //
        // Save all the new and updated partitions
        //
        log.info("Saving partitions({}) for {}", partitions.size(), name);
        final SavePartitionResult savePartitionResult = splitManager
            .savePartitions(tableHandle, partitions, partitionIdsForDeletes,
                checkIfExists, alterIfExists);

        // Save metadata
        log.info("Saving user metadata for partitions for {}", name);
        // delete metadata
        if (!deletePartitions.isEmpty()) {
            log.info("Deleting user metadata for partitions with names {} for {}", partitionIdsForDeletes, name);
            deleteMetadatas(session.getUser(), deletePartitions);
        }
        userMetadataService.saveMetadatas(session.getUser(), partitionDtos, true);

        result.setUpdated(savePartitionResult.getUpdated());
        result.setAdded(savePartitionResult.getAdded());

        return result;
    }

    @Override
    public void delete(final QualifiedName name, final List<String> partitionIds) {
        final TagList tags = BasicTagList
            .of("catalog", name.getCatalogName(), "database", name.getDatabaseName(), "table", name.getTableName());
        DynamicGauge.set(LogConstants.GaugeDeletePartitions.toString(), tags, partitionIds.size());
        final Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (!tableHandle.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(name.getDatabaseName(), name.getTableName()));
        }
        if (!partitionIds.isEmpty()) {
            final Session session = sessionProvider.getSession(name);
            final ConnectorPartitionResult partitionResult = splitManager
                .getPartitions(tableHandle.get(), null, partitionIds, null, null, false);
            log.info("Deleting partitions with names {} for {}", partitionIds, name);
            splitManager.deletePartitions(tableHandle.get(), partitionIds);
            final List<HasMetadata> partitions = partitionResult.getPartitions().stream()
                .map(partition -> toPartitionDto(name, partition))
                .collect(Collectors.toList());
            // delete metadata
            log.info("Deleting user metadata for partitions with names {} for {}", partitionIds, name);
            deleteMetadatas(session.getUser(), partitions);
        }
    }

    private void deleteMetadatas(final String userId, final List<HasMetadata> partitions) {
        // Spawning off since this is a time consuming task
        threadServiceManager.getExecutor().submit(() -> userMetadataService.deleteMetadatas(userId, partitions));
    }

    @Override
    public List<QualifiedName> getQualifiedNames(final String uri, final boolean prefixSearch) {
        return getQualifiedNames(Lists.newArrayList(uri), prefixSearch).values().stream().flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    @Override
    public Map<String, List<QualifiedName>> getQualifiedNames(final List<String> uris, final boolean prefixSearch) {
        final Map<String, List<QualifiedName>> result = Maps.newConcurrentMap();
        final List<ListenableFuture<Void>> futures = Lists.newArrayList();
        catalogService.getCatalogNames().forEach(catalog -> {
            final Session session = sessionProvider.getSession(QualifiedName.ofCatalog(catalog.getCatalogName()));
            futures.add(threadServiceManager.getExecutor().submit(() -> {
                final Map<String, List<SchemaTablePartitionName>> schemaTablePartitionNames = splitManager
                    .getPartitionNames(session, uris, prefixSearch);
                schemaTablePartitionNames.forEach((uri, schemaTablePartitionNames1) -> {
                    final List<QualifiedName> partitionNames = schemaTablePartitionNames1.stream().map(
                        schemaTablePartitionName -> QualifiedName.ofPartition(catalog.getConnectorName(),
                            schemaTablePartitionName.getTableName().getSchemaName(),
                            schemaTablePartitionName.getTableName().getTableName(),
                            schemaTablePartitionName.getPartitionId())).collect(Collectors.toList());
                    final List<QualifiedName> existingPartitionNames = result.get(uri);
                    if (existingPartitionNames == null) {
                        result.put(uri, partitionNames);
                    } else {
                        existingPartitionNames.addAll(partitionNames);
                    }
                });
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
    public List<String> getPartitionKeys(final QualifiedName name, final String filter,
        final List<String> partitionNames, final Sort sort, final Pageable pageable) {
        List<String> result = Lists.newArrayList();
        final Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (tableHandle.isPresent()) {
            result = splitManager.getPartitionKeys(tableHandle.get(), filter, partitionNames, sort, pageable);
        }
        return result;
    }

    @Override
    public List<String> getPartitionUris(final QualifiedName name, final String filter,
        final List<String> partitionNames, final Sort sort, final Pageable pageable) {
        List<String> result = Lists.newArrayList();
        final Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (tableHandle.isPresent()) {
            result = splitManager.getPartitionUris(tableHandle.get(), filter, partitionNames, sort, pageable);
        }
        return result;
    }

    @Override
    public void create(
        @Nonnull
        final QualifiedName name,
        @Nonnull
        final PartitionDto dto) {
        save(name, Lists.newArrayList(dto), null, false, false);
    }

    @Override
    public void update(
        @Nonnull
        final QualifiedName name,
        @Nonnull
        final PartitionDto dto) {
        save(name, Lists.newArrayList(dto), null, true, false);
    }

    @Override
    public void delete(
        @Nonnull
        final QualifiedName name) {
        final QualifiedName tableName = QualifiedName
            .ofTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
        delete(tableName, Lists.newArrayList(name.getPartitionName()));
    }

    @Override
    public PartitionDto get(
        @Nonnull
        final QualifiedName name) {
        PartitionDto result = null;
        final QualifiedName tableName = QualifiedName
            .ofTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
        final List<PartitionDto> dtos =
            list(tableName, null, Lists.newArrayList(name.getPartitionName()), null, null, true, true, true);
        if (!dtos.isEmpty()) {
            result = dtos.get(0);
        }
        return result;
    }

    @Override
    public boolean exists(
        @Nonnull
        final QualifiedName name) {
        return get(name) != null;
    }

    private PartitionDto toPartitionDto(final QualifiedName tableName, final ConnectorPartition partition) {
        final QualifiedName partitionName = QualifiedName.ofPartition(
            tableName.getCatalogName(),
            tableName.getDatabaseName(),
            tableName.getTableName(),
            partition.getPartitionId()
        );
        return prestoConverters.toPartitionDto(partitionName, partition);
    }
}
