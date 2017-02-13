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
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatCreateMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateMViewPreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatSaveMViewPartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveMViewPartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateMViewPostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateMViewPreEvent;
import com.netflix.metacat.common.server.exception.NotFoundException;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Metacat view service.
 */
@Slf4j
public class MViewServiceImpl implements MViewService {
    /** Hive database name where views are stored. */
    public static final String VIEW_DB_NAME = "franklinviews";
    private static final List<String> SUPPORTED_SOURCES = Lists.newArrayList("hive", "s3", "aegisthus");
    private final ConnectorManager connectorManager;
    private final TableService tableService;
    private final PartitionService partitionService;
    private final UserMetadataService userMetadataService;
    private final MetacatEventBus eventBus;
    private final ConverterUtil converterUtil;

    /**
     * Constructor.
     * @param connectorManager connector manager
     * @param tableService table service
     * @param partitionService partition service
     * @param userMetadataService user metadata service
     * @param eventBus Internal event bus
     * @param converterUtil utility to convert to/from Dto to connector resources
     */
    @Inject
    public MViewServiceImpl(final ConnectorManager connectorManager,
        final TableService tableService,
        final PartitionService partitionService,
        final UserMetadataService userMetadataService, final MetacatEventBus eventBus,
        final ConverterUtil converterUtil) {
        this.connectorManager = connectorManager;
        this.tableService = tableService;
        this.partitionService = partitionService;
        this.userMetadataService = userMetadataService;
        this.eventBus = eventBus;
        this.converterUtil = converterUtil;
    }

    /**
     * Creates the materialized view using the schema of the give table
     * Assumes that the "franklinviews" database name already exists in the given catalog.
     */
    @Override
    public TableDto create(@Nonnull final QualifiedName name) {
        return createAndSnapshotPartitions(name, false, null);
    }

    /**
     * Creates the materialized view using the schema of the give table
     * Assumes that the "franklinviews" database name already exists in the given catalog.
     */
    @Override
    public TableDto createAndSnapshotPartitions(@Nonnull final QualifiedName name,
        final boolean snapshot,
        final String filter) {
        TableDto result = null;
        // Get the table
        log.info("Get the table {}", name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatCreateMViewPreEvent(name, metacatRequestContext, snapshot, filter));
        final Optional<TableDto> oTable = tableService.get(name, false);
        if (oTable.isPresent()) {
            final TableDto table = oTable.get();
            final String viewName = createViewName(name);
            final QualifiedName targetName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, viewName);
            // Get the view table if it exists
            log.info("Check if the view table {} exists.", targetName);
            Optional<TableDto> oViewTable = Optional.empty();
            try {
                oViewTable = tableService.get(targetName, false);
            } catch (NotFoundException ignored) {

            }
            if (!oViewTable.isPresent()) {
                log.info("Creating view {}.", targetName);
                result = tableService.copy(table, targetName);
            } else {
                result = oViewTable.get();
            }
            if (snapshot) {
                snapshotPartitions(name, filter);
            }
            eventBus.postAsync(new MetacatCreateMViewPostEvent(name, metacatRequestContext, result, snapshot, filter));
        } else {
            throw new TableNotFoundException(name);
        }
        return result;
    }

    @Override
    public TableDto create(@Nonnull final QualifiedName name, @Nonnull final TableDto dto) {
        // Ignore the dto passed
        return create(name);
    }

    @Override
    public TableDto deleteAndReturn(@Nonnull final QualifiedName name) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatDeleteMViewPreEvent(name, metacatRequestContext));
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        log.info("Deleting view {}.", viewQName);
        final TableDto deletedDto = tableService.deleteAndReturn(viewQName, true);
        eventBus.postAsync(new MetacatDeleteMViewPostEvent(name, metacatRequestContext, deletedDto));
        return deletedDto;
    }

    @Override
    public void delete(@Nonnull final QualifiedName name) {
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        tableService.deleteAndReturn(viewQName, true);
    }

    @Override
    public void update(@Nonnull final QualifiedName name, @Nonnull final TableDto tableDto) {
        updateAndReturn(name, tableDto);
    }

    @Override
    public TableDto updateAndReturn(@Nonnull final QualifiedName name, @Nonnull final TableDto tableDto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatUpdateMViewPreEvent(name, metacatRequestContext, tableDto));
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        log.info("Updating view {}.", viewQName);
        tableService.update(viewQName, tableDto);
        final TableDto updatedDto = getOpt(name).orElseThrow(() -> new IllegalStateException("should exist"));
        eventBus.postAsync(new MetacatUpdateMViewPostEvent(name, metacatRequestContext, updatedDto));
        return updatedDto;
    }
    @Override
    public TableDto get(@Nonnull final QualifiedName name) {
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return tableService.get(viewQName);
    }

    @Override
    public Optional<TableDto> getOpt(@Nonnull final QualifiedName name) {
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        final Optional<TableDto> result = tableService.get(viewQName, false);

        //
        // User definition metadata of the underlying table is returned
        //
        if (result.isPresent()) {
            final TableDto table = result.get();
            table.setName(name);
            final QualifiedName tableName = QualifiedName
                .ofTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
            final Optional<ObjectNode> definitionMetadata = userMetadataService.getDefinitionMetadata(tableName);
            if (definitionMetadata.isPresent()) {
                userMetadataService.populateMetadata(table, definitionMetadata.get(), null);
            }
        }
        return result;
    }

    @Override
    public void snapshotPartitions(@Nonnull final QualifiedName name, final String filter) {
        final List<PartitionDto> partitionDtos =
            partitionService.list(name, filter, null, null, null, false, false, true);
        if (partitionDtos != null && !partitionDtos.isEmpty()) {
            log.info("Snapshot partitions({}) for view {}.", partitionDtos.size(), name);
            final PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
            dto.setPartitions(partitionDtos);
            savePartitions(name, dto, false);
        }
    }

    @Override
    public PartitionsSaveResponseDto savePartitions(@Nonnull final QualifiedName name,
        final PartitionsSaveRequestDto dto, final boolean merge) {
        PartitionsSaveResponseDto result = null;
        final List<PartitionDto> partitionDtos = dto.getPartitions();
        if (partitionDtos == null || partitionDtos.isEmpty()) {
            return new PartitionsSaveResponseDto();
        }
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        partitionDtos.forEach(partitionDto ->
            partitionDto.setName(QualifiedName
                .ofPartition(viewQName.getCatalogName(), viewQName.getDatabaseName(), viewQName.getTableName(),
                    partitionDto.getName().getPartitionName())));
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatSaveMViewPartitionPreEvent(name, metacatRequestContext, dto));
        final List<String> partitionIdsForDeletes = dto.getPartitionIdsForDeletes();
        if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            eventBus.postSync(new MetacatDeleteMViewPartitionPreEvent(name, metacatRequestContext, dto));
        }
        if (merge) {
            final List<String> partitionNames = partitionDtos.stream().map(
                partitionDto -> partitionDto.getName().getPartitionName()).collect(Collectors.toList());
            final List<PartitionDto> existingPartitions = partitionService
                .list(viewQName, null, partitionNames, null, null, false, false, false);
            final Map<String, PartitionDto> existingPartitionsMap = existingPartitions.stream()
                .collect(Collectors
                    .toMap(partitionDto -> partitionDto.getName().getPartitionName(), Function.identity()));
            final List<PartitionDto> mergedPartitions = partitionDtos.stream()
                .map(partitionDto -> {
                    final String partitionName = partitionDto.getName().getPartitionName();
                    final PartitionDto existingPartition = existingPartitionsMap.get(partitionName);
                    return mergePartition(partitionDto, existingPartition);
                }).collect(Collectors.toList());
            dto.setPartitions(mergedPartitions);
            result =  partitionService.save(viewQName, dto);
        } else {
            result = partitionService.save(viewQName, dto);
        }
        eventBus.postAsync(new MetacatSaveMViewPartitionPostEvent(name, metacatRequestContext, dto.getPartitions()));
        if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
            eventBus.postAsync(
                new MetacatDeleteMViewPartitionPostEvent(name, metacatRequestContext, partitionIdsForDeletes));
        }
        return result;
    }

    private PartitionDto mergePartition(final PartitionDto partitionDto, final PartitionDto existingPartition) {
        if (existingPartition != null) {
            final StorageDto existingSerde = existingPartition.getSerde();
            if (existingSerde != null) {
                StorageDto serde = partitionDto.getSerde();
                if (serde == null) {
                    serde = new StorageDto();
                    partitionDto.setSerde(serde);
                }
                if (serde.getUri() == null || serde.getUri().equals(existingSerde.getUri())) {
                    serde.setUri(existingSerde.getUri());
                    if (serde.getInputFormat() == null) {
                        serde.setInputFormat(existingSerde.getInputFormat());
                    }
                    if (serde.getOutputFormat() == null) {
                        serde.setOutputFormat(existingSerde.getOutputFormat());
                    }
                    if (serde.getSerializationLib() == null) {
                        serde.setSerializationLib(existingSerde.getSerializationLib());
                    }
                }
            }
        }
        return partitionDto;
    }

    @Override
    public void deletePartitions(@Nonnull final QualifiedName name, final List<String> partitionIds) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
        dto.setPartitionIdsForDeletes(partitionIds);
        eventBus.postSync(new MetacatDeleteMViewPartitionPreEvent(name, metacatRequestContext, dto));
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        partitionService.delete(viewQName, partitionIds);
        eventBus.postAsync(new MetacatDeleteMViewPartitionPostEvent(name, metacatRequestContext, partitionIds));
    }

    @Override
    public List<PartitionDto> listPartitions(@Nonnull final QualifiedName name, final String filter,
        final List<String> partitionNames, final Sort sort,
        final Pageable pageable, final boolean includeUserMetadata, final boolean includePartitionDetails) {
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return partitionService
            .list(viewQName, filter, partitionNames, sort, pageable, includeUserMetadata, includeUserMetadata,
                includePartitionDetails);
    }

    @Override
    public List<String> getPartitionKeys(@Nonnull final QualifiedName name, final String filter,
        final List<String> partitionNames, final Sort sort, final Pageable pageable) {
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return partitionService.getPartitionKeys(viewQName, filter, partitionNames, sort, pageable);
    }

    @Override
    public List<String> getPartitionUris(@Nonnull final QualifiedName name, final String filter,
        final List<String> partitionNames, final Sort sort, final Pageable pageable) {
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return partitionService.getPartitionUris(viewQName, filter, partitionNames, sort, pageable);
    }

    @Override
    public Integer partitionCount(@Nonnull final QualifiedName name) {
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return partitionService.count(viewQName);
    }

    @Override
    public List<NameDateDto> list(@Nonnull final QualifiedName name) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final QualifiedName viewDbName = QualifiedName.ofDatabase(name.getCatalogName(), VIEW_DB_NAME);
        final ConnectorTableService service = connectorManager.getTableService(name.getCatalogName());

        List<QualifiedName> tableNames = Lists.newArrayList();
        try {
            final ConnectorContext connectorContext = converterUtil.toConnectorContext(metacatRequestContext);
            tableNames = service.listNames(connectorContext, viewDbName, null, null, null);
        } catch (Exception ignored) {
            // ignore. Return an empty list if database 'franklinviews' does not exist
        }
        if (!name.isDatabaseDefinition() && name.isCatalogDefinition()) {
            return tableNames.stream()
                .map(viewName -> {
                    final NameDateDto dto = new NameDateDto();
                    dto.setName(viewName);
                    return dto;
                })
                .collect(Collectors.toList());
        } else {
            final String prefix = String.format("%s_%s_", name.getDatabaseName(),
                MoreObjects.firstNonNull(name.getTableName(), ""));
            return tableNames.stream()
                .filter(qualifiedTableName -> qualifiedTableName.getTableName().startsWith(prefix))
                .map(qualifiedTableName -> {
                    final NameDateDto dto = new NameDateDto();
                    dto.setName(QualifiedName
                        .ofView(qualifiedTableName.getCatalogName(), name.getDatabaseName(), name.getTableName(),
                            qualifiedTableName.getTableName().substring(prefix.length())));
                    return dto;
                })
                .collect(Collectors.toList());
        }
    }

    @Override
    public void saveMetadata(@Nonnull final QualifiedName name, final ObjectNode definitionMetadata,
        final ObjectNode dataMetadata) {
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        tableService.saveMetadata(viewQName, definitionMetadata, dataMetadata);
    }

    @Override
    public void rename(@Nonnull  final QualifiedName name, @Nonnull final QualifiedName newViewName) {
        final QualifiedName oldViewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        final QualifiedName newViewQName = QualifiedName
            .ofTable(newViewName.getCatalogName(), VIEW_DB_NAME, createViewName(newViewName));
        tableService.rename(oldViewQName, newViewQName, true);
    }

    @Override
    public boolean exists(@Nonnull final QualifiedName name) {
        final QualifiedName viewQName =
            QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return tableService.exists(viewQName);
    }

    /**
     * The view is going to be represented by a table in a special db in Franklin.  As such there must be
     * a conversion from view id -> view table id like so:
     * [dbName]_[tableName]_[viewName]
     */
    private String createViewName(final QualifiedName name) {
        return String.format("%s_%s_%s", name.getDatabaseName(), name.getTableName(), name.getViewName());
    }
}
