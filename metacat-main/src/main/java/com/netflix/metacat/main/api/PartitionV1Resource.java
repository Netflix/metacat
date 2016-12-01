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

package com.netflix.metacat.main.api;

import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.Sort;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.api.PartitionV1;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteMViewPartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatSaveMViewPartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveMViewPartitionPreEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPostEvent;
import com.netflix.metacat.common.server.events.MetacatSaveTablePartitionPreEvent;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.PartitionService;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.netflix.metacat.main.api.RequestWrapper.qualifyName;
import static com.netflix.metacat.main.api.RequestWrapper.requestWrapper;

public class PartitionV1Resource implements PartitionV1 {
    private final MViewService mViewService;
    private final MetacatEventBus eventBus;
    private final MetacatV1 v1;
    private final PartitionService partitionService;

    @Inject
    public PartitionV1Resource(
        MetacatEventBus eventBus,
        MetacatV1 v1,
        MViewService mViewService,
        PartitionService partitionService) {
        this.eventBus = eventBus;
        this.v1 = v1;
        this.mViewService = mViewService;
        this.partitionService = partitionService;
    }

    @Override
    public void deletePartitions(String catalogName, String databaseName, String tableName, List<String> partitionIds) {
        MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        requestWrapper(name, "deleteTablePartition", () -> {
            if (partitionIds == null || partitionIds.isEmpty()) {
                throw new IllegalArgumentException("partitionIds are required");
            }

            PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
            dto.setPartitionIdsForDeletes(partitionIds);
            eventBus.postSync(new MetacatDeleteTablePartitionPreEvent(name, metacatRequestContext, dto));

            partitionService.delete(name, partitionIds);

            // This metadata is actually for the table, if it is present update that
            if (dto.getDefinitionMetadata() != null
                || dto.getDataMetadata() != null) {
                TableDto tableDto = v1.getTable(catalogName, databaseName, tableName, false, false, false);
                tableDto.setDefinitionMetadata(dto.getDefinitionMetadata());
                tableDto.setDataMetadata(dto.getDataMetadata());
                v1.updateTable(catalogName, databaseName, tableName, tableDto);
            }

            eventBus.postAsync(new MetacatDeleteTablePartitionPostEvent(name, metacatRequestContext, partitionIds));
            return null;
        });
    }

    @Override
    public void deletePartitions(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        List<String> partitionIds) {
        MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        requestWrapper(name, "deleteMViewPartition", () -> {
            if (partitionIds == null || partitionIds.isEmpty()) {
                throw new IllegalArgumentException("partitionIds are required");
            }

            PartitionsSaveRequestDto dto = new PartitionsSaveRequestDto();
            dto.setPartitionIdsForDeletes(partitionIds);
            eventBus.postSync(new MetacatDeleteMViewPartitionPreEvent(name, metacatRequestContext, dto));

            mViewService.deletePartitions(name, partitionIds);

            // This metadata is actually for the view, if it is present update that
            if (dto.getDefinitionMetadata() != null
                || dto.getDataMetadata() != null) {
                TableDto tableDto = v1.getMView(catalogName, databaseName, tableName, viewName);
                tableDto.setDefinitionMetadata(dto.getDefinitionMetadata());
                tableDto.setDataMetadata(dto.getDataMetadata());
                v1.updateMView(catalogName, databaseName, tableName, viewName, tableDto);
            }

            eventBus.postAsync(new MetacatDeleteMViewPartitionPostEvent(name, metacatRequestContext, partitionIds));
            return null;
        });
    }

    @Override
    public Integer getPartitionCount(
        String catalogName,
        String databaseName,
        String tableName) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "getPartitionCount", () -> partitionService.count(name));
    }

    @Override
    public Integer getPartitionCount(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "getPartitionCount", () -> mViewService.partitionCount(name));
    }

    @Override
    public List<PartitionDto> getPartitions(
        String catalogName,
        String databaseName,
        String tableName,
        String filter,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        Boolean includeUserMetadata) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "getPartitions", () -> {
            com.facebook.presto.spi.SortOrder order = null;
            if (sortOrder != null) {
                order = com.facebook.presto.spi.SortOrder.valueOf(sortOrder.name());
            }
            return partitionService.list(
                name,
                filter,
                null,
                new Sort(sortBy, order),
                new Pageable(limit, offset),
                includeUserMetadata,
                includeUserMetadata,
                false
            );
        });
    }

    private List<PartitionDto> getPartitions(
        String catalogName,
        String databaseName,
        String tableName,
        String filter,
        List<String> partitionNames,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        Boolean includeUserMetadata,
        Boolean includePartitionDetails) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "getPartitions", () -> {
            com.facebook.presto.spi.SortOrder order = null;
            if (sortOrder != null) {
                order = com.facebook.presto.spi.SortOrder.valueOf(sortOrder.name());
            }
            return partitionService.list(
                name,
                filter,
                partitionNames,
                new Sort(sortBy, order),
                new Pageable(limit, offset),
                includeUserMetadata,
                includeUserMetadata,
                includePartitionDetails
            );
        });
    }

    @Override
    public List<PartitionDto> getPartitions(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        String filter,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        Boolean includeUserMetadata) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "getPartitions", () -> {
            com.facebook.presto.spi.SortOrder order = null;
            if (sortOrder != null) {
                order = com.facebook.presto.spi.SortOrder.valueOf(sortOrder.name());
            }
            return mViewService.listPartitions(
                name,
                filter,
                null,
                new Sort(sortBy, order),
                new Pageable(limit, offset),
                includeUserMetadata,
                false
            );
        });
    }

    private List<PartitionDto> getPartitions(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        String filter,
        List<String> partitionNames,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        Boolean includeUserMetadata,
        Boolean includePartitionDetails) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "getPartitions", () -> {
            com.facebook.presto.spi.SortOrder order = null;
            if (sortOrder != null) {
                order = com.facebook.presto.spi.SortOrder.valueOf(sortOrder.name());
            }
            return mViewService.listPartitions(
                name,
                filter,
                partitionNames,
                new Sort(sortBy, order),
                new Pageable(limit, offset),
                includeUserMetadata,
                includePartitionDetails
            );
        });
    }

    @Override
    public List<String> getPartitionKeysForRequest(
        String catalogName,
        String databaseName,
        String tableName,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        GetPartitionsRequestDto getPartitionsRequestDto) {
        String filterExpression = null;
        List<String> partitionNames = null;
        if (getPartitionsRequestDto != null) {
            filterExpression = getPartitionsRequestDto.getFilter();
            partitionNames = getPartitionsRequestDto.getPartitionNames();
        }
        return _getPartitionKeys(catalogName, databaseName, tableName, filterExpression, partitionNames, sortBy,
            sortOrder, offset, limit);
    }

    private List<String> _getPartitionKeys(
        String catalogName,
        String databaseName,
        String tableName,
        String filter,
        List<String> partitionNames,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "getPartitionKeys", () -> {
            com.facebook.presto.spi.SortOrder order = null;
            if (sortOrder != null) {
                order = com.facebook.presto.spi.SortOrder.valueOf(sortOrder.name());
            }
            return partitionService.getPartitionKeys(
                name,
                filter,
                partitionNames,
                new Sort(sortBy, order),
                new Pageable(limit, offset)
            );
        });
    }

    private List<String> _getPartitionUris(
        String catalogName,
        String databaseName,
        String tableName,
        String filter,
        List<String> partitionNames,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return requestWrapper(name, "getMViewPartitionUris", () -> {
            com.facebook.presto.spi.SortOrder order = null;
            if (sortOrder != null) {
                order = com.facebook.presto.spi.SortOrder.valueOf(sortOrder.name());
            }
            return partitionService.getPartitionUris(
                name,
                filter,
                partitionNames,
                new Sort(sortBy, order),
                new Pageable(limit, offset)
            );
        });
    }

    private List<String> _getMViewPartitionKeys(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        String filter,
        List<String> partitionNames,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "getMViewPartitionKeys", () -> {
            com.facebook.presto.spi.SortOrder order = null;
            if (sortOrder != null) {
                order = com.facebook.presto.spi.SortOrder.valueOf(sortOrder.name());
            }
            return mViewService.getPartitionKeys(
                name,
                filter,
                partitionNames,
                new Sort(sortBy, order),
                new Pageable(limit, offset)
            );
        });
    }

    private List<String> _getMViewPartitionUris(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        String filter,
        List<String> partitionNames,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit) {
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "getMViewPartitionUris", () -> {
            com.facebook.presto.spi.SortOrder order = null;
            if (sortOrder != null) {
                order = com.facebook.presto.spi.SortOrder.valueOf(sortOrder.name());
            }
            return mViewService.getPartitionUris(
                name,
                filter,
                partitionNames,
                new Sort(sortBy, order),
                new Pageable(limit, offset)
            );
        });
    }

    @Override
    public List<String> getPartitionUrisForRequest(
        String catalogName,
        String databaseName,
        String tableName,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        GetPartitionsRequestDto getPartitionsRequestDto) {
        String filterExpression = null;
        List<String> partitionNames = null;
        if (getPartitionsRequestDto != null) {
            filterExpression = getPartitionsRequestDto.getFilter();
            partitionNames = getPartitionsRequestDto.getPartitionNames();
        }
        return _getPartitionUris(catalogName, databaseName, tableName, filterExpression, partitionNames, sortBy,
            sortOrder, offset, limit);
    }

    @Override
    public List<String> getPartitionKeys(
        String catalogName,
        String databaseName,
        String tableName,
        String filter,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit) {
        return _getPartitionKeys(catalogName, databaseName, tableName, filter, null, sortBy, sortOrder, offset, limit);
    }

    @Override
    public List<String> getPartitionKeys(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        String filter,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit) {
        return _getMViewPartitionKeys(catalogName, databaseName, tableName, viewName, filter, null, sortBy, sortOrder,
            offset, limit);
    }

    @Override
    public List<PartitionDto> getPartitionsForRequest(
        String catalogName,
        String databaseName,
        String tableName,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        Boolean includeUserMetadata,
        GetPartitionsRequestDto getPartitionsRequestDto) {
        String filterExpression = null;
        List<String> partitionNames = null;
        Boolean includePartitionDetails = false;
        if (getPartitionsRequestDto != null) {
            filterExpression = getPartitionsRequestDto.getFilter();
            partitionNames = getPartitionsRequestDto.getPartitionNames();
            includePartitionDetails = getPartitionsRequestDto.getIncludePartitionDetails();
        }
        return getPartitions(catalogName, databaseName, tableName, filterExpression, partitionNames, sortBy, sortOrder,
            offset, limit,
            includeUserMetadata, includePartitionDetails);
    }

    @Override
    public List<String> getPartitionKeysForRequest(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        GetPartitionsRequestDto getPartitionsRequestDto) {
        String filterExpression = null;
        List<String> partitionNames = null;
        if (getPartitionsRequestDto != null) {
            filterExpression = getPartitionsRequestDto.getFilter();
            partitionNames = getPartitionsRequestDto.getPartitionNames();
        }
        return _getMViewPartitionKeys(catalogName, databaseName, tableName, viewName, filterExpression, partitionNames,
            sortBy, sortOrder, offset, limit);
    }

    @Override
    public List<String> getPartitionUris(
        String catalogName,
        String databaseName,
        String tableName,
        String filter,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit) {
        return _getPartitionUris(catalogName, databaseName, tableName, filter, null, sortBy, sortOrder, offset, limit);
    }

    @Override
    public List<String> getPartitionUris(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        String filter,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit) {
        return _getMViewPartitionUris(catalogName, databaseName, tableName, viewName, filter, null, sortBy, sortOrder,
            offset, limit);
    }

    @Override
    public List<PartitionDto> getPartitionsForRequest(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        Boolean includeUserMetadata,
        GetPartitionsRequestDto getPartitionsRequestDto) {
        String filterExpression = null;
        List<String> partitionNames = null;
        Boolean includePartitionDetails = false;
        if (getPartitionsRequestDto != null) {
            filterExpression = getPartitionsRequestDto.getFilter();
            partitionNames = getPartitionsRequestDto.getPartitionNames();
            includePartitionDetails = getPartitionsRequestDto.getIncludePartitionDetails();
        }
        return getPartitions(catalogName, databaseName, tableName, viewName, filterExpression, partitionNames, sortBy,
            sortOrder,
            offset, limit, includeUserMetadata, includePartitionDetails);
    }

    @Override
    public List<String> getPartitionUrisForRequest(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        String sortBy,
        SortOrder sortOrder,
        Integer offset,
        Integer limit,
        GetPartitionsRequestDto getPartitionsRequestDto) {
        String filterExpression = null;
        List<String> partitionNames = null;
        if (getPartitionsRequestDto != null) {
            filterExpression = getPartitionsRequestDto.getFilter();
            partitionNames = getPartitionsRequestDto.getPartitionNames();
        }
        return _getMViewPartitionUris(catalogName, databaseName, tableName, viewName, filterExpression, partitionNames,
            sortBy, sortOrder, offset, limit);
    }

    @Override
    public PartitionsSaveResponseDto savePartitions(
        String catalogName,
        String databaseName,
        String tableName,
        PartitionsSaveRequestDto partitionsSaveRequestDto) {
        MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        QualifiedName name = QualifiedName.ofTable(catalogName, databaseName, tableName);
        return requestWrapper(name, "saveTablePartition", () -> {
            checkArgument(partitionsSaveRequestDto != null &&
                    partitionsSaveRequestDto.getPartitions() != null &&
                    !partitionsSaveRequestDto.getPartitions().isEmpty(),
                "Partitions must be present");

            eventBus
                .postSync(new MetacatSaveTablePartitionPreEvent(name, metacatRequestContext, partitionsSaveRequestDto));

            List<PartitionDto> partitionsToSave = partitionsSaveRequestDto.getPartitions();
            boolean checkIfExists = partitionsSaveRequestDto.getCheckIfExists() == null
                || partitionsSaveRequestDto.getCheckIfExists();
            boolean alterIfExists = partitionsSaveRequestDto.getAlterIfExists() != null
                && partitionsSaveRequestDto.getAlterIfExists();
            List<String> partitionIdsForDeletes = partitionsSaveRequestDto.getPartitionIdsForDeletes();
            if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
                eventBus.postSync(
                    new MetacatDeleteTablePartitionPreEvent(name, metacatRequestContext, partitionsSaveRequestDto));
            }

            PartitionsSaveResponseDto result = partitionService.save(name, partitionsToSave, partitionIdsForDeletes,
                checkIfExists, alterIfExists);

            // This metadata is actually for the table, if it is present update that
            if (partitionsSaveRequestDto.getDefinitionMetadata() != null
                || partitionsSaveRequestDto.getDataMetadata() != null) {
                TableDto dto = v1.getTable(catalogName, databaseName, tableName, true, false, false);
                dto.setDefinitionMetadata(partitionsSaveRequestDto.getDefinitionMetadata());
                dto.setDataMetadata(partitionsSaveRequestDto.getDataMetadata());
                v1.updateTable(catalogName, databaseName, tableName, dto);
            }

            eventBus.postAsync(
                new MetacatSaveTablePartitionPostEvent(name, metacatRequestContext, partitionsToSave, result));
            if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
                eventBus.postAsync(
                    new MetacatDeleteTablePartitionPostEvent(name, metacatRequestContext, partitionIdsForDeletes));
            }
            return result;
        });
    }

    @Override
    public PartitionsSaveResponseDto savePartitions(
        String catalogName,
        String databaseName,
        String tableName,
        String viewName,
        PartitionsSaveRequestDto partitionsSaveRequestDto) {
        MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "saveMViewPartition", () -> {
            checkArgument(partitionsSaveRequestDto != null &&
                    partitionsSaveRequestDto.getPartitions() != null &&
                    !partitionsSaveRequestDto.getPartitions().isEmpty(),
                "Partitions must be present");

            eventBus
                .postSync(new MetacatSaveMViewPartitionPreEvent(name, metacatRequestContext, partitionsSaveRequestDto));

            List<PartitionDto> partitionsToSave = partitionsSaveRequestDto.getPartitions();
            boolean checkIfExists = partitionsSaveRequestDto.getCheckIfExists() == null
                || partitionsSaveRequestDto.getCheckIfExists();
            boolean alterIfExists = partitionsSaveRequestDto.getAlterIfExists() != null
                && partitionsSaveRequestDto.getAlterIfExists();
            List<String> partitionIdsForDeletes = partitionsSaveRequestDto.getPartitionIdsForDeletes();
            if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
                eventBus.postSync(
                    new MetacatDeleteMViewPartitionPreEvent(name, metacatRequestContext, partitionsSaveRequestDto));
            }

            PartitionsSaveResponseDto result = mViewService
                .savePartitions(name, partitionsToSave, partitionIdsForDeletes, true,
                    checkIfExists, alterIfExists);

            // This metadata is actually for the view, if it is present update that
            if (partitionsSaveRequestDto.getDefinitionMetadata() != null
                || partitionsSaveRequestDto.getDataMetadata() != null) {
                TableDto dto = v1.getMView(catalogName, databaseName, tableName, viewName);
                dto.setDefinitionMetadata(partitionsSaveRequestDto.getDefinitionMetadata());
                dto.setDataMetadata(partitionsSaveRequestDto.getDataMetadata());
                v1.updateMView(catalogName, databaseName, tableName, viewName, dto);
            }

            eventBus.postAsync(new MetacatSaveMViewPartitionPostEvent(name, metacatRequestContext, partitionsToSave));
            if (partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()) {
                eventBus.postAsync(
                    new MetacatDeleteMViewPartitionPostEvent(name, metacatRequestContext, partitionIdsForDeletes));
            }
            return result;
        });
    }
}
