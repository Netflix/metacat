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
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveResponse;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionMetadataOnlyPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionMetadataOnlyPreEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPreEvent;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
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
    private final Registry registry;
    private final Id partitionAddDistSummary;
    private final Id partitionMetadataOnlyAddDistSummary;
    private final Id partitionGetDistSummary;
    private final Id partitionDeleteDistSummary;

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
     * @param registry             registry handle
     */
    public PartitionServiceImpl(
        final CatalogService catalogService,
        final ConnectorManager connectorManager,
        final TableService tableService,
        final UserMetadataService userMetadataService,
        final ThreadServiceManager threadServiceManager,
        final Config config,
        final MetacatEventBus eventBus,
        final ConverterUtil converterUtil,
        final Registry registry
    ) {
        this.catalogService = catalogService;
        this.connectorManager = connectorManager;
        this.tableService = tableService;
        this.userMetadataService = userMetadataService;
        this.threadServiceManager = threadServiceManager;
        this.config = config;
        this.eventBus = eventBus;
        this.converterUtil = converterUtil;
        this.registry = registry;
        this.partitionAddDistSummary =
            registry.createId(Metrics.DistributionSummaryAddPartitions.getMetricName());
        this.partitionMetadataOnlyAddDistSummary =
            registry.createId(Metrics.DistributionSummaryMetadataOnlyAddPartitions.getMetricName());
        this.partitionGetDistSummary =
            registry.createId(Metrics.DistributionSummaryGetPartitions.getMetricName());
        this.partitionDeleteDistSummary =
            registry.createId(Metrics.DistributionSummaryDeletePartitions.getMetricName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<PartitionDto> list(
        final QualifiedName name,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable,
        final boolean includeUserDefinitionMetadata,
        final boolean includeUserDataMetadata,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        // the conversion will handle getPartitionsRequestDto as null case
        final PartitionListRequest partitionListRequest =
            converterUtil.toPartitionListRequest(getPartitionsRequestDto, pageable, sort);
        final String filterExpression = partitionListRequest.getFilter();
        final List<String> partitionNames = partitionListRequest.getPartitionNames();

        if (Strings.isNullOrEmpty(filterExpression)
            && (pageable == null || !pageable.isPageable())
            && (partitionNames == null || partitionNames.isEmpty())
            && config.getNamesToThrowErrorOnListPartitionsWithNoFilter().contains(name)) {
            throw new IllegalArgumentException(String.format("No filter or limit specified for table %s", name));
        }

        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());

        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        final List<PartitionInfo> resultInfo = service
            .getPartitions(connectorRequestContext, name, partitionListRequest);


        List<PartitionDto> result = Lists.newArrayList();
        if (resultInfo != null && !resultInfo.isEmpty()) {
            result = resultInfo.stream().map(converterUtil::toPartitionDto).collect(Collectors.toList());
            final List<QualifiedName> names = Lists.newArrayList();
            final List<String> uris = Lists.newArrayList();
            result.forEach(partitionDto -> {
                names.add(partitionDto.getName());
                uris.add(partitionDto.getDataUri());
            });

            registry.distributionSummary(
                this.partitionGetDistSummary.withTags(name.parts())).record(result.size());

            log.info("Got {} partitions for {} using filter: {} and partition names: {}",
                result.size(), name, filterExpression,
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

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer count(final QualifiedName name) {
        Integer result = 0;
        if (tableService.exists(name)) {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            result = service.getPartitionCount(connectorRequestContext, name);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionsSaveResponseDto save(final QualifiedName name, final PartitionsSaveRequestDto dto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
        final List<PartitionDto> partitionDtos = dto.getPartitions();
        // If no partitions are passed, then return
        if (partitionDtos == null || partitionDtos.isEmpty()) {
            return new PartitionsSaveResponseDto();
        }
        if (!tableService.exists(name)) {
            throw new TableNotFoundException(name);
        }
        //optimization for metadata only updates (e.g. squirrel) , assuming only validate partitions are requested
        if (dto.getSaveMetadataOnly()) {
            return savePartitionMetadataOnly(metacatRequestContext, dto, name, partitionDtos);
        } else {
            return updatePartitions(service, metacatRequestContext, dto, name, partitionDtos);
        }
    }

    /**
     * Optimization for metadata only updates.
     *
     * @param metacatRequestContext request context
     * @param dto                   savePartition dto
     * @param name                  qualified name
     * @param partitionDtos         partition dtos
     * @return empty save partition response dto
     */
    private PartitionsSaveResponseDto savePartitionMetadataOnly(
        final MetacatRequestContext metacatRequestContext,
        final PartitionsSaveRequestDto dto,
        final QualifiedName name, final List<PartitionDto> partitionDtos) {
        registry.distributionSummary(
            this.partitionMetadataOnlyAddDistSummary.withTags(name.parts())).record(partitionDtos.size());
        eventBus.postSync(
            new MetacatSaveTablePartitionMetadataOnlyPreEvent(name, metacatRequestContext, this, dto));
        // Save metadata
        log.info("Saving metadata only for partitions for {}", name);
        userMetadataService.saveMetadata(metacatRequestContext.getUserName(), partitionDtos, true);
        eventBus.postSync(
            new MetacatSaveTablePartitionMetadataOnlyPostEvent(
                name, metacatRequestContext, this, partitionDtos, new PartitionsSaveResponseDto()));
        //empty saveResponseDto is returned for optimization purpose
        //since client (squirrel) only checks the response code
        return converterUtil.toPartitionsSaveResponseDto(new PartitionsSaveResponse());
    }

    /**
     * Add, delete, update partitions.
     *
     * @param service               partition service
     * @param metacatRequestContext metacat request context
     * @param dto                   partition save request dto
     * @param name                  qualified name
     * @param partitionDtos         partitions dto
     * @return partition save response dto
     */
    private PartitionsSaveResponseDto updatePartitions(
        final ConnectorPartitionService service,
        final MetacatRequestContext metacatRequestContext,
        final PartitionsSaveRequestDto dto,
        final QualifiedName name, final List<PartitionDto> partitionDtos) {
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        List<HasMetadata> deletePartitions = Lists.newArrayList();
        registry.distributionSummary(
            this.partitionAddDistSummary.withTags(name.parts())).record(partitionDtos.size());
        final List<String> partitionIdsForDeletes = dto.getPartitionIdsForDeletes();
        if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            eventBus.postSync(new MetacatDeleteTablePartitionPreEvent(name, metacatRequestContext, this, dto));
            registry.distributionSummary(
                this.partitionDeleteDistSummary.withTags(name.parts())).record(partitionIdsForDeletes.size());
            final GetPartitionsRequestDto requestDto =
                new GetPartitionsRequestDto(null, partitionIdsForDeletes, false, true);
            final List<PartitionInfo> deletePartitionInfos = service.getPartitions(connectorRequestContext, name,
                converterUtil.toPartitionListRequest(requestDto, null, null));
            if (deletePartitionInfos != null) {
                deletePartitions = deletePartitionInfos.stream().map(converterUtil::toPartitionDto)
                    .collect(Collectors.toList());
            }
        }

        // Save all the new and updated partitions
        eventBus.postSync(new MetacatSaveTablePartitionPreEvent(name, metacatRequestContext, this, dto));
        log.info("Saving partitions for {} ({})", name, partitionDtos.size());
        final PartitionsSaveResponseDto result = converterUtil.toPartitionsSaveResponseDto(
            service.savePartitions(connectorRequestContext, name, converterUtil.toPartitionsSaveRequest(dto)));
        // Save metadata
        log.info("Saving user metadata for partitions for {}", name);
        // delete metadata
        if (!deletePartitions.isEmpty()) {
            log.info("Deleting user metadata for partitions with names {} for {}", partitionIdsForDeletes, name);
            deleteMetadatas(metacatRequestContext.getUserName(), deletePartitions);
        }
        final long start = registry.clock().wallTime();
        userMetadataService.saveMetadata(metacatRequestContext.getUserName(), partitionDtos, true);
        final long duration = registry.clock().wallTime() - start;
        log.info("Time taken to save user metadata for table {} is {} ms", name, duration);
        registry.timer(registry.createId(Metrics.TimerSavePartitionMetadata.getMetricName()).withTags(name.parts()))
            .record(duration, TimeUnit.MILLISECONDS);
        eventBus.postAsync(
            new MetacatSaveTablePartitionPostEvent(name, metacatRequestContext, this, partitionDtos, result));
        if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            eventBus.postAsync(
                new MetacatDeleteTablePartitionPostEvent(name,
                    metacatRequestContext, this, partitionIdsForDeletes));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final QualifiedName name, final List<String> partitionIds) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        registry.distributionSummary(
            this.partitionDeleteDistSummary.withTags(name.parts())).record(partitionIds.size());
        if (!tableService.exists(name)) {
            throw new TableNotFoundException(name);
        }
        if (!partitionIds.isEmpty()) {
            final PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
            dto.setPartitionIdsForDeletes(partitionIds);
            eventBus.postSync(new MetacatDeleteTablePartitionPreEvent(name, metacatRequestContext, this, dto));
            final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
            // Get the partitions before calling delete
            final GetPartitionsRequestDto requestDto = new GetPartitionsRequestDto(null, partitionIds, false, true);
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            final List<PartitionInfo> partitionInfos = service.getPartitions(connectorRequestContext, name,
                converterUtil.toPartitionListRequest(requestDto, null, null));
            List<HasMetadata> partitions = Lists.newArrayList();
            if (partitionInfos != null) {
                partitions = partitionInfos.stream().map(converterUtil::toPartitionDto).collect(Collectors.toList());
            }
            log.info("Deleting partitions with names {} for {}", partitionIds, name);
            service.deletePartitions(connectorRequestContext, name, partitionIds);

            // delete metadata
            log.info("Deleting user metadata for partitions with names {} for {}", partitionIds, name);
            if (!partitions.isEmpty()) {
                deleteMetadatas(metacatRequestContext.getUserName(), partitions);
            }
            eventBus.postAsync(
                new MetacatDeleteTablePartitionPostEvent(name, metacatRequestContext, this, partitionIds)
            );
        }
    }

    private void deleteMetadatas(final String userId, final List<HasMetadata> partitions) {
        // Spawning off since this is a time consuming task
        threadServiceManager.getExecutor().submit(() -> userMetadataService.deleteMetadata(userId, partitions));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> getQualifiedNames(final String uri, final boolean prefixSearch) {
        return getQualifiedNames(Lists.newArrayList(uri), prefixSearch).values().stream().flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<QualifiedName>> getQualifiedNames(final List<String> uris, final boolean prefixSearch) {
        final Map<String, List<QualifiedName>> result = Maps.newConcurrentMap();
        final List<ListenableFuture<Void>> futures = Lists.newArrayList();
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        catalogService.getCatalogNames().forEach(catalog -> {
            futures.add(threadServiceManager.getExecutor().submit(() -> {
                final ConnectorPartitionService service =
                    connectorManager.getPartitionService(catalog.getCatalogName());
                final ConnectorRequestContext connectorRequestContext
                    = converterUtil.toConnectorContext(metacatRequestContext);
                try {
                    final Map<String, List<QualifiedName>> partitionNames = service
                        .getPartitionNames(connectorRequestContext, uris, prefixSearch);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getPartitionKeys(
        final QualifiedName name,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        List<String> result = Lists.newArrayList();
        if (tableService.exists(name)) {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            try {
                result = service.getPartitionKeys(
                    connectorRequestContext,
                    name,
                    converterUtil.toPartitionListRequest(getPartitionsRequestDto, pageable, sort)
                );
            } catch (final UnsupportedOperationException uoe) {
                log.debug("Catalog {} doesn't support getPartitionKeys. Ignoring.", name.getCatalogName());
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getPartitionUris(
        final QualifiedName name,
        @Nullable final Sort sort,
        @Nullable final Pageable pageable,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    ) {
        List<String> result = Lists.newArrayList();
        if (tableService.exists(name)) {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorPartitionService service = connectorManager.getPartitionService(name.getCatalogName());
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            try {
                result = service.getPartitionUris(connectorRequestContext, name,
                    converterUtil.toPartitionListRequest(getPartitionsRequestDto, pageable, sort));
            } catch (final UnsupportedOperationException uoe) {
                log.info("Catalog {} doesn't support getPartitionUris. Ignoring.", name.getCatalogName());
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionDto create(final QualifiedName name, final PartitionDto partitionDto) {
        final PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
        dto.setCheckIfExists(false);
        dto.setPartitions(Lists.newArrayList(partitionDto));
        save(name, dto);
        return partitionDto;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(final QualifiedName name, final PartitionDto partitionDto) {
        final PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
        dto.setPartitions(Lists.newArrayList(partitionDto));
        save(name, dto);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionDto updateAndReturn(final QualifiedName name, final PartitionDto dto) {
        update(name, dto);
        return dto;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final QualifiedName name) {
        final QualifiedName tableName = QualifiedName
            .ofTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
        delete(tableName, Lists.newArrayList(name.getPartitionName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionDto get(final QualifiedName name) {
        PartitionDto result = null;
        final QualifiedName tableName = QualifiedName
            .ofTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
        final List<PartitionDto> dtos =
            list(tableName, null, null, true, true,
                new GetPartitionsRequestDto(null, Lists.newArrayList(name.getPartitionName()), true, true));
        if (!dtos.isEmpty()) {
            result = dtos.get(0);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final QualifiedName name) {
        return get(name) != null;
    }
}
