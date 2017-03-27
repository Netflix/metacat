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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.HasMetadata;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.monitoring.DynamicGauge;
import com.netflix.metacat.common.server.monitoring.LogConstants;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPreEvent;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import com.netflix.servo.tag.BasicTagList;
import com.netflix.servo.tag.TagList;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Partition service.
 */
@Slf4j
public class PartitionServiceImpl implements PartitionService {
    private final CatalogService catalogService;
    private final ConnectorManager connectorManager;
    private final TableService tableService;
    private final UserMetadataService userMetadataService;
    private final ThreadServiceManager threadServiceManager;
    private final Config config;
    private final MetacatEventBus eventBus;
    private final ConverterUtil converterUtil;

    /**
     * Constructor.
     *
     * @param catalogService       catalog service
     * @param connectorManager     connector manager
     * @param tableService         table service
     * @param userMetadataService  user metadata service
     * @param threadServiceManager thread manager
     * @param config               configurations
     * @param eventBus             Internal event bus
     * @param converterUtil        utility to convert to/from Dto to connector resources
     */
    @Inject
    public PartitionServiceImpl(
        final CatalogService catalogService,
        final ConnectorManager connectorManager,
        final TableService tableService,
        final UserMetadataService userMetadataService,
        final ThreadServiceManager threadServiceManager,
        final Config config,
        final MetacatEventBus eventBus,
        final ConverterUtil converterUtil
    ) {
        this.catalogService = catalogService;
        this.connectorManager = connectorManager;
        this.tableService = tableService;
        this.userMetadataService = userMetadataService;
        this.threadServiceManager = threadServiceManager;
        this.config = config;
        this.eventBus = eventBus;
        this.converterUtil = converterUtil;
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
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
        final GetPartitionsRequestDto requestDto = new GetPartitionsRequestDto();
        requestDto.setFilter(filter);
        requestDto.setIncludePartitionDetails(includePartitionDetails);
        requestDto.setPartitionNames(partitionNames);
        final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
        final List<PartitionInfo> resultInfo = service
            .getPartitions(connectorContext, name, converterUtil.toPartitionListRequest(requestDto, pageable, sort));
        List<PartitionDto> result = Lists.newArrayList();
        if (resultInfo != null && !resultInfo.isEmpty()) {
            result = resultInfo.stream().map(converterUtil::toPartitionDto).collect(Collectors.toList());
            final List<QualifiedName> names = Lists.newArrayList();
            final List<String> uris = Lists.newArrayList();
            result.forEach(partitionDto -> {
                names.add(partitionDto.getName());
                uris.add(partitionDto.getDataUri());
            });
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
        if (tableService.exists(name)) {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
            final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
            result = service.getPartitionCount(connectorContext, name);
        }
        return result;
    }

    @Override
    public PartitionsSaveResponseDto save(@Nonnull final QualifiedName name, final PartitionsSaveRequestDto dto) {
        PartitionsSaveResponseDto result = new PartitionsSaveResponseDto();
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
        final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
        final List<PartitionDto> partitionDtos = dto.getPartitions();
        // If no partitions are passed, then return
        if (partitionDtos == null || partitionDtos.isEmpty()) {
            return result;
        }
        final List<String> partitionIdsForDeletes = dto.getPartitionIdsForDeletes();
        final TagList tags = BasicTagList.of("catalog", name.getCatalogName(),
            "database", name.getDatabaseName(), "table",
            name.getTableName());
        DynamicGauge.set(LogConstants.GaugeAddPartitions.toString(), tags, partitionDtos.size());
        if (!tableService.exists(name)) {
            throw new TableNotFoundException(name);
        }
        List<HasMetadata> deletePartitions = Lists.newArrayList();
        if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            eventBus.postSync(new MetacatDeleteTablePartitionPreEvent(name, metacatRequestContext, dto));
            DynamicGauge.set(LogConstants.GaugeDeletePartitions.toString(), tags, partitionIdsForDeletes.size());
            final GetPartitionsRequestDto requestDto = new GetPartitionsRequestDto();
            requestDto.setIncludePartitionDetails(false);
            requestDto.setPartitionNames(partitionIdsForDeletes);
            final List<PartitionInfo> deletePartitionInfos = service.getPartitions(connectorContext, name,
                converterUtil.toPartitionListRequest(requestDto, null, null));
            if (deletePartitionInfos != null) {
                deletePartitions = deletePartitionInfos.stream().map(converterUtil::toPartitionDto)
                    .collect(Collectors.toList());
            }
        }
        //
        // Save all the new and updated partitions
        //
        eventBus
            .postSync(new MetacatSaveTablePartitionPreEvent(name, metacatRequestContext, dto));
        log.info("Saving partitions({}) for {}", partitionDtos.size(), name);
        result = converterUtil.toPartitionsSaveResponseDto(
            service.savePartitions(connectorContext, name, converterUtil.toPartitionsSaveRequest(dto)));

        // Save metadata
        log.info("Saving user metadata for partitions for {}", name);
        // delete metadata
        if (!deletePartitions.isEmpty()) {
            log.info("Deleting user metadata for partitions with names {} for {}", partitionIdsForDeletes, name);
            deleteMetadatas(metacatRequestContext.getUserName(), deletePartitions);
        }
        userMetadataService.saveMetadatas(metacatRequestContext.getUserName(), partitionDtos, true);
        eventBus.postAsync(
            new MetacatSaveTablePartitionPostEvent(name, metacatRequestContext, partitionDtos, result));
        if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            eventBus.postAsync(
                new MetacatDeleteTablePartitionPostEvent(name, metacatRequestContext, partitionIdsForDeletes));
        }

        return result;
    }

    @Override
    public void delete(final QualifiedName name, final List<String> partitionIds) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final TagList tags = BasicTagList
            .of("catalog", name.getCatalogName(), "database", name.getDatabaseName(), "table", name.getTableName());
        DynamicGauge.set(LogConstants.GaugeDeletePartitions.toString(), tags, partitionIds.size());
        if (!tableService.exists(name)) {
            throw new TableNotFoundException(name);
        }
        if (!partitionIds.isEmpty()) {
            final PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
            dto.setPartitionIdsForDeletes(partitionIds);
            eventBus.postSync(new MetacatDeleteTablePartitionPreEvent(name, metacatRequestContext, dto));
            final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
            // Get the partitions before calling delete
            final GetPartitionsRequestDto requestDto = new GetPartitionsRequestDto();
            requestDto.setIncludePartitionDetails(false);
            requestDto.setPartitionNames(partitionIds);
            final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
            final List<PartitionInfo> partitionInfos = service.getPartitions(connectorContext, name,
                converterUtil.toPartitionListRequest(requestDto, null, null));
            List<HasMetadata> partitions = Lists.newArrayList();
            if (partitionInfos != null) {
                partitions = partitionInfos.stream().map(converterUtil::toPartitionDto).collect(Collectors.toList());
            }
            log.info("Deleting partitions with names {} for {}", partitionIds, name);
            service.deletePartitions(connectorContext, name, partitionIds);

            // delete metadata
            log.info("Deleting user metadata for partitions with names {} for {}", partitionIds, name);
            if (!partitions.isEmpty()) {
                deleteMetadatas(metacatRequestContext.getUserName(), partitions);
            }
            eventBus.postAsync(new MetacatDeleteTablePartitionPostEvent(name, metacatRequestContext, partitionIds));
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
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        catalogService.getCatalogNames().forEach(catalog -> {
            futures.add(threadServiceManager.getExecutor().submit(() -> {
                final ConnectorPartitionService service =
                    connectorManager.getPartitionService(catalog.getCatalogName());
                final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
                try {
                    final Map<String, List<QualifiedName>> partitionNames = service
                        .getPartitionNames(connectorContext, uris, prefixSearch);
                    partitionNames.forEach((uri, subPartitionNames) -> {
                        final List<QualifiedName> existingPartitionNames = result.get(uri);
                        if (existingPartitionNames == null) {
                            result.put(uri, subPartitionNames);
                        } else {
                            existingPartitionNames.addAll(subPartitionNames);
                        }
                    });
                } catch (final UnsupportedOperationException uoe) {
                    log.debug("Catalog {} doesn't support getPartitionNames. Ignoring.", catalog.getCatalogName());
                }
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
    public List<String> getPartitionKeys(
        final QualifiedName name,
        final String filter,
        final List<String> partitionNames,
        final Sort sort,
        final Pageable pageable
    ) {
        List<String> result = Lists.newArrayList();
        if (tableService.exists(name)) {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
            final GetPartitionsRequestDto requestDto = new GetPartitionsRequestDto();
            requestDto.setFilter(filter);
            requestDto.setPartitionNames(partitionNames);
            final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
            try {
                result = service.getPartitionKeys(
                    connectorContext,
                    name,
                    converterUtil.toPartitionListRequest(requestDto, pageable, sort)
                );
            } catch (final UnsupportedOperationException uoe) {
                log.debug("Catalog {} doesn't support getPartitionKeys. Ignoring.", name.getCatalogName());
            }
        }
        return result;
    }

    @Override
    public List<String> getPartitionUris(
        final QualifiedName name,
        final String filter,
        final List<String> partitionNames,
        final Sort sort,
        final Pageable pageable
    ) {
        List<String> result = Lists.newArrayList();
        if (tableService.exists(name)) {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
            final GetPartitionsRequestDto requestDto = new GetPartitionsRequestDto();
            requestDto.setFilter(filter);
            requestDto.setPartitionNames(partitionNames);
            final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
            try {
                result = service.getPartitionUris(connectorContext, name,
                    converterUtil.toPartitionListRequest(requestDto, pageable, sort));
            } catch (final UnsupportedOperationException uoe) {
                log.info("Catalog {} doesn't support getPartitionUris. Ignoring.", name.getCatalogName());
            }
        }
        return result;
    }

    @Override
    public PartitionDto create(@Nonnull final QualifiedName name, @Nonnull final PartitionDto partitionDto) {
        final PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
        dto.setCheckIfExists(false);
        dto.setPartitions(Lists.newArrayList(partitionDto));
        save(name, dto);
        return partitionDto;
    }

    @Override
    public void update(@Nonnull final QualifiedName name, @Nonnull final PartitionDto partitionDto) {
        final PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
        dto.setPartitions(Lists.newArrayList(partitionDto));
        save(name, dto);
    }

    @Override
    public PartitionDto updateAndReturn(@Nonnull final QualifiedName name, @Nonnull final PartitionDto dto) {
        update(name, dto);
        return dto;
    }

    @Override
    public void delete(@Nonnull final QualifiedName name) {
        final QualifiedName tableName = QualifiedName
            .ofTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
        delete(tableName, Lists.newArrayList(name.getPartitionName()));
    }

    @Override
    public PartitionDto get(@Nonnull final QualifiedName name) {
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
    public boolean exists(@Nonnull final QualifiedName name) {
        return get(name) != null;
    }
}
