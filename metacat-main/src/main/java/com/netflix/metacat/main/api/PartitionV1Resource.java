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

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.api.PartitionV1;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.PartitionService;

import javax.inject.Inject;
import java.util.List;

/**
 * Partition V1 API implementation.
 */
public class PartitionV1Resource implements PartitionV1 {
    private final MViewService mViewService;
    private final MetacatV1 v1;
    private final PartitionService partitionService;

    /**
     * Constructor.
     * @param eventBus event bus
     * @param v1 Metacat V1
     * @param mViewService view service
     * @param partitionService partition service
     */
    @Inject
    public PartitionV1Resource(
        final MetacatEventBus eventBus,
        final MetacatV1 v1,
        final MViewService mViewService,
        final PartitionService partitionService) {
        this.v1 = v1;
        this.mViewService = mViewService;
        this.partitionService = partitionService;
    }

    @Override
    public void deletePartitions(final String catalogName, final String databaseName, final String tableName,
        final List<String> partitionIds) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        RequestWrapper.requestWrapper(name, "deleteTablePartition", () -> {
            if (partitionIds == null || partitionIds.isEmpty()) {
                throw new IllegalArgumentException("partitionIds are required");
            }
            partitionService.delete(name, partitionIds);
            return null;
        });
    }

    @Override
    public void deletePartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final List<String> partitionIds) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        RequestWrapper.requestWrapper(name, "deleteMViewPartition", () -> {
            if (partitionIds == null || partitionIds.isEmpty()) {
                throw new IllegalArgumentException("partitionIds are required");
            }
            mViewService.deletePartitions(name, partitionIds);
            return null;
        });
    }

    @Override
    public Integer getPartitionCount(
        final String catalogName,
        final String databaseName,
        final String tableName) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "getPartitionCount", () -> partitionService.count(name));
    }

    @Override
    public Integer getPartitionCount(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "getPartitionCount", () -> mViewService.partitionCount(name));
    }

    @Override
    public List<PartitionDto> getPartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String filter,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final Boolean includeUserMetadata) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "getPartitions", () -> partitionService.list(
            name,
            filter,
            null,
            new Sort(sortBy, sortOrder),
            new Pageable(limit, offset),
            includeUserMetadata,
            includeUserMetadata,
            false
        ));
    }

    private List<PartitionDto> getPartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String filter,
        final List<String> partitionNames,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final Boolean includeUserMetadata,
        final Boolean includePartitionDetails) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "getPartitions", () -> partitionService.list(
            name,
            filter,
            partitionNames,
            new Sort(sortBy, sortOrder),
            new Pageable(limit, offset),
            includeUserMetadata,
            includeUserMetadata,
            includePartitionDetails
        ));
    }

    @Override
    public List<PartitionDto> getPartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final String filter,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final Boolean includeUserMetadata) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "getPartitions", () -> mViewService.listPartitions(
            name,
            filter,
            null,
            new Sort(sortBy, sortOrder),
            new Pageable(limit, offset),
            includeUserMetadata,
            false
        ));
    }

    private List<PartitionDto> getPartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final String filter,
        final List<String> partitionNames,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final Boolean includeUserMetadata,
        final Boolean includePartitionDetails) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "getPartitions", () -> mViewService.listPartitions(
            name,
            filter,
            partitionNames,
            new Sort(sortBy, sortOrder),
            new Pageable(limit, offset),
            includeUserMetadata,
            includePartitionDetails
        ));
    }

    @Override
    public List<String> getPartitionKeysForRequest(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final GetPartitionsRequestDto getPartitionsRequestDto) {
        String filterExpression = null;
        List<String> partitionNames = null;
        if (getPartitionsRequestDto != null) {
            filterExpression = getPartitionsRequestDto.getFilter();
            partitionNames = getPartitionsRequestDto.getPartitionNames();
        }
        return _getPartitionKeys(catalogName, databaseName, tableName, filterExpression, partitionNames, sortBy,
            sortOrder, offset, limit);
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<String> _getPartitionKeys(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String filter,
        final List<String> partitionNames,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "getPartitionKeys", () -> partitionService.getPartitionKeys(
            name,
            filter,
            partitionNames,
            new Sort(sortBy, sortOrder),
            new Pageable(limit, offset)
        ));
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<String> _getPartitionUris(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String filter,
        final List<String> partitionNames,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        return RequestWrapper.requestWrapper(name, "getMViewPartitionUris", () -> partitionService.getPartitionUris(
            name,
            filter,
            partitionNames,
            new Sort(sortBy, sortOrder),
            new Pageable(limit, offset)
        ));
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<String> _getMViewPartitionKeys(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final String filter,
        final List<String> partitionNames,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "getMViewPartitionKeys", () -> mViewService.getPartitionKeys(
            name,
            filter,
            partitionNames,
            new Sort(sortBy, sortOrder),
            new Pageable(limit, offset)
        ));
    }

    @SuppressWarnings("checkstyle:methodname")
    private List<String> _getMViewPartitionUris(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final String filter,
        final List<String> partitionNames,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "getMViewPartitionUris", () -> mViewService.getPartitionUris(
            name,
            filter,
            partitionNames,
            new Sort(sortBy, sortOrder),
            new Pageable(limit, offset)
        ));
    }

    @Override
    public List<String> getPartitionUrisForRequest(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final GetPartitionsRequestDto getPartitionsRequestDto) {
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
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String filter,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit) {
        return _getPartitionKeys(catalogName, databaseName, tableName, filter, null, sortBy, sortOrder, offset, limit);
    }

    @Override
    public List<String> getPartitionKeys(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final String filter,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit) {
        return _getMViewPartitionKeys(catalogName, databaseName, tableName, viewName, filter, null, sortBy, sortOrder,
            offset, limit);
    }

    @Override
    public List<PartitionDto> getPartitionsForRequest(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final Boolean includeUserMetadata,
        final GetPartitionsRequestDto getPartitionsRequestDto) {
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
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final GetPartitionsRequestDto getPartitionsRequestDto) {
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
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String filter,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit) {
        return _getPartitionUris(catalogName, databaseName, tableName, filter, null, sortBy, sortOrder, offset, limit);
    }

    @Override
    public List<String> getPartitionUris(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final String filter,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit) {
        return _getMViewPartitionUris(catalogName, databaseName, tableName, viewName, filter, null, sortBy, sortOrder,
            offset, limit);
    }

    @Override
    public List<PartitionDto> getPartitionsForRequest(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final Boolean includeUserMetadata,
        final GetPartitionsRequestDto getPartitionsRequestDto) {
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
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final String sortBy,
        final SortOrder sortOrder,
        final Integer offset,
        final Integer limit,
        final GetPartitionsRequestDto getPartitionsRequestDto) {
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
        final String catalogName,
        final String databaseName,
        final String tableName,
        final PartitionsSaveRequestDto partitionsSaveRequestDto) {
        final QualifiedName name = QualifiedName.ofTable(catalogName, databaseName, tableName);
        return RequestWrapper.requestWrapper(name, "saveTablePartition", () -> {
            final PartitionsSaveResponseDto result;
            if (partitionsSaveRequestDto == null || partitionsSaveRequestDto.getPartitions() == null
                || partitionsSaveRequestDto.getPartitions().isEmpty()) {
                result = new PartitionsSaveResponseDto();
            } else {
                result = partitionService.save(name, partitionsSaveRequestDto);

                // This metadata is actually for the table, if it is present update that
                if (partitionsSaveRequestDto.getDefinitionMetadata() != null
                    || partitionsSaveRequestDto.getDataMetadata() != null) {
                    final TableDto dto = v1.getTable(catalogName, databaseName, tableName, true, false, false);
                    dto.setDefinitionMetadata(partitionsSaveRequestDto.getDefinitionMetadata());
                    dto.setDataMetadata(partitionsSaveRequestDto.getDataMetadata());
                    v1.updateTable(catalogName, databaseName, tableName, dto);
                }
            }
            return result;
        });
    }

    @Override
    public PartitionsSaveResponseDto savePartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final String viewName,
        final PartitionsSaveRequestDto partitionsSaveRequestDto) {
        final QualifiedName name =
            RequestWrapper.qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return RequestWrapper.requestWrapper(name, "saveMViewPartition", () -> {
            final PartitionsSaveResponseDto result;
            if (partitionsSaveRequestDto == null || partitionsSaveRequestDto.getPartitions() == null
                || partitionsSaveRequestDto.getPartitions().isEmpty()) {
                result = new PartitionsSaveResponseDto();
            } else {

                result = mViewService.savePartitions(name, partitionsSaveRequestDto, true);
                // This metadata is actually for the view, if it is present update that
                if (partitionsSaveRequestDto.getDefinitionMetadata() != null
                    || partitionsSaveRequestDto.getDataMetadata() != null) {
                    final TableDto dto = v1.getMView(catalogName, databaseName, tableName, viewName);
                    dto.setDefinitionMetadata(partitionsSaveRequestDto.getDefinitionMetadata());
                    dto.setDataMetadata(partitionsSaveRequestDto.getDataMetadata());
                    v1.updateMView(catalogName, databaseName, tableName, viewName, dto);
                }
            }
            return result;
        });
    }
}
