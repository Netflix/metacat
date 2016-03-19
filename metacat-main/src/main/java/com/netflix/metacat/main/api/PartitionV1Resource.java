package com.netflix.metacat.main.api;

import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.Sort;
import com.netflix.metacat.common.MetacatContext;
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
import java.util.stream.Collectors;

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
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofTable(catalogName, databaseName, tableName));
        requestWrapper(name, "deleteTablePartition", () -> {
            if (partitionIds == null || partitionIds.isEmpty()) {
                throw new IllegalArgumentException("partitionIds are required");
            }

            eventBus.post(new MetacatDeleteTablePartitionPreEvent(name, partitionIds, metacatContext));

            partitionService.delete(name, partitionIds);

            eventBus.post(new MetacatDeleteTablePartitionPostEvent(name, partitionIds, metacatContext));
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
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        requestWrapper(name, "deleteMViewPartition", () -> {
            if (partitionIds == null || partitionIds.isEmpty()) {
                throw new IllegalArgumentException("partitionIds are required");
            }

            eventBus.post(new MetacatDeleteMViewPartitionPreEvent(name, partitionIds, metacatContext));

            mViewService.deletePartitions(name, partitionIds);

            eventBus.post(new MetacatDeleteMViewPartitionPostEvent(name, partitionIds, metacatContext));
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
            Boolean includeUserMetadata,
            GetPartitionsRequestDto getPartitionsRequestDto) {
        List<PartitionDto> result = getPartitionsForRequest(catalogName, databaseName, tableName, sortBy, sortOrder,
                offset, limit, includeUserMetadata, getPartitionsRequestDto);
        return result.stream().map(partitionDto -> partitionDto.getName().getPartitionName()).collect(Collectors.toList());
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
            Boolean includeUserMetadata,
            GetPartitionsRequestDto getPartitionsRequestDto) {
        List<PartitionDto> result = getPartitionsForRequest(catalogName, databaseName, tableName, sortBy, sortOrder,
                offset, limit, includeUserMetadata, getPartitionsRequestDto);
        return result.stream().map(PartitionDto::getDataUri).collect(Collectors.toList());
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
            Integer limit,
            Boolean includeUserMetadata) {
        List<PartitionDto> result = getPartitions(catalogName, databaseName, tableName, filter, sortBy, sortOrder,
                offset, limit, includeUserMetadata);
        return result.stream().map(partitionDto -> partitionDto.getName().getPartitionName()).collect(Collectors.toList());
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
            Integer limit,
            Boolean includeUserMetadata) {
        List<PartitionDto> result = getPartitions(catalogName, databaseName, tableName, viewName, filter, sortBy,
                sortOrder, offset, limit, includeUserMetadata);
        return result.stream().map(partitionDto -> partitionDto.getName().getPartitionName()).collect(Collectors.toList());
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
        if( getPartitionsRequestDto != null){
            filterExpression = getPartitionsRequestDto.getFilter();
            partitionNames = getPartitionsRequestDto.getPartitionNames();
            includePartitionDetails = getPartitionsRequestDto.getIncludePartitionDetails();
        }
        return getPartitions(catalogName, databaseName, tableName, filterExpression, partitionNames, sortBy, sortOrder, offset, limit,
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
            Boolean includeUserMetadata,
            GetPartitionsRequestDto getPartitionsRequestDto) {
        List<PartitionDto> result = getPartitionsForRequest(catalogName, databaseName, tableName, viewName, sortBy,
                sortOrder, offset, limit, includeUserMetadata, getPartitionsRequestDto);
        return result.stream().map(partitionDto -> partitionDto.getName().getPartitionName()).collect(Collectors.toList());
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
            Integer limit,
            Boolean includeUserMetadata) {
        List<PartitionDto> result = getPartitions(catalogName, databaseName, tableName, filter, sortBy, sortOrder,
                offset, limit, includeUserMetadata);
        return result.stream().map(PartitionDto::getDataUri).collect(Collectors.toList());
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
            Integer limit,
            Boolean includeUserMetadata) {
        List<PartitionDto> result = getPartitions(catalogName, databaseName, tableName, viewName, filter, sortBy,
                sortOrder, offset, limit, includeUserMetadata);
        return result.stream().map(PartitionDto::getDataUri).collect(Collectors.toList());
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
        if( getPartitionsRequestDto != null){
            filterExpression = getPartitionsRequestDto.getFilter();
            partitionNames = getPartitionsRequestDto.getPartitionNames();
            includePartitionDetails = getPartitionsRequestDto.getIncludePartitionDetails();
        }
        return getPartitions(catalogName, databaseName, tableName, viewName, filterExpression, partitionNames, sortBy, sortOrder,
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
            Boolean includeUserMetadata,
            GetPartitionsRequestDto getPartitionsRequestDto) {
        List<PartitionDto> result = getPartitionsForRequest(catalogName, databaseName, tableName, viewName, sortBy,
                sortOrder, offset, limit, includeUserMetadata, getPartitionsRequestDto);
        return result.stream().map(PartitionDto::getDataUri).collect(Collectors.toList());
    }

    @Override
    public PartitionsSaveResponseDto savePartitions(
            String catalogName,
            String databaseName,
            String tableName,
            PartitionsSaveRequestDto partitionsSaveRequestDto) {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = QualifiedName.ofTable(catalogName, databaseName, tableName);
        return requestWrapper(name, "saveTablePartition", () -> {
            checkArgument(partitionsSaveRequestDto != null &&
                            partitionsSaveRequestDto.getPartitions() != null &&
                            !partitionsSaveRequestDto.getPartitions().isEmpty(),
                    "Partitions must be present");

            List<PartitionDto> partitionsToSave = partitionsSaveRequestDto.getPartitions();
            boolean checkIfExists = partitionsSaveRequestDto.getCheckIfExists() == null?true:partitionsSaveRequestDto.getCheckIfExists();
            eventBus.post(new MetacatSaveTablePartitionPreEvent(name, partitionsToSave, metacatContext));
            List<String> partitionIdsForDeletes = partitionsSaveRequestDto.getPartitionIdsForDeletes();
            if( partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()){
                eventBus.post(new MetacatDeleteTablePartitionPreEvent(name, partitionIdsForDeletes, metacatContext));
            }

            PartitionsSaveResponseDto result = partitionService.save(name, partitionsToSave, partitionIdsForDeletes, checkIfExists);

            // This metadata is actually for the table, if it is present update that
            if (partitionsSaveRequestDto.getDefinitionMetadata() != null
                    || partitionsSaveRequestDto.getDataMetadata() != null) {
                TableDto dto = v1.getTable(catalogName, databaseName, tableName, true, false, false);
                dto.setDefinitionMetadata(partitionsSaveRequestDto.getDefinitionMetadata());
                dto.setDataMetadata(partitionsSaveRequestDto.getDataMetadata());
                v1.updateTable(catalogName, databaseName, tableName, dto);
            }

            eventBus.post(new MetacatSaveTablePartitionPostEvent(name, partitionsToSave, metacatContext));
            if( partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()){
                eventBus.post(new MetacatDeleteTablePartitionPostEvent(name, partitionIdsForDeletes, metacatContext));
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
        MetacatContext metacatContext = MetacatContextManager.getContext();
        QualifiedName name = qualifyName(() -> QualifiedName.ofView(catalogName, databaseName, tableName, viewName));
        return requestWrapper(name, "saveMViewPartition", () -> {
            checkArgument(partitionsSaveRequestDto != null &&
                            partitionsSaveRequestDto.getPartitions() != null &&
                            !partitionsSaveRequestDto.getPartitions().isEmpty(),
                    "Partitions must be present");

            List<PartitionDto> partitionsToSave = partitionsSaveRequestDto.getPartitions();
            boolean checkIfExists = partitionsSaveRequestDto.getCheckIfExists() == null?true:partitionsSaveRequestDto.getCheckIfExists();
            eventBus.post(new MetacatSaveMViewPartitionPreEvent(name, partitionsToSave, metacatContext));
            List<String> partitionIdsForDeletes = partitionsSaveRequestDto.getPartitionIdsForDeletes();
            if( partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()){
                eventBus.post(new MetacatDeleteMViewPartitionPreEvent(name, partitionIdsForDeletes, metacatContext));
            }

            PartitionsSaveResponseDto result = mViewService.savePartitions(name, partitionsToSave, partitionIdsForDeletes, true, checkIfExists);

            // This metadata is actually for the view, if it is present update that
            if (partitionsSaveRequestDto.getDefinitionMetadata() != null
                    || partitionsSaveRequestDto.getDataMetadata() != null) {
                TableDto dto = v1.getMView(catalogName, databaseName, tableName, viewName);
                dto.setDefinitionMetadata(partitionsSaveRequestDto.getDefinitionMetadata());
                dto.setDataMetadata(partitionsSaveRequestDto.getDataMetadata());
                v1.updateMView(catalogName, databaseName, tableName, viewName, dto);
            }

            eventBus.post(new MetacatSaveMViewPartitionPostEvent(name, partitionsToSave, metacatContext));
            if( partitionIdsForDeletes != null && !partitionIdsForDeletes.isEmpty()){
                eventBus.post(new MetacatDeleteMViewPartitionPostEvent(name, partitionIdsForDeletes, metacatContext));
            }
            return result;
        });
    }
}
