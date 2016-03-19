package com.netflix.metacat.main.services.impl;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.SavePartitionResult;
import com.facebook.presto.spi.SchemaTablePartitionName;
import com.facebook.presto.spi.Sort;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.HasMetadata;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import com.netflix.metacat.common.monitoring.DynamicGauge;
import com.netflix.metacat.common.monitoring.LogConstants;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
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

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
                Map<String,ObjectNode> dataMetadataMap = includeUserDataMetadata?userMetadataService.getDataMetadataMap(uris):
                        Maps.newHashMap();
                Map<String,ObjectNode> definitionMetadataMap = includeUserDefinitionMetadata?userMetadataService.getDefinitionMetadataMap(names):
                        Maps.newHashMap();
                result.stream().forEach(partitionDto -> userMetadataService.populateMetadata(partitionDto
                        , definitionMetadataMap.get(partitionDto.getName().toString())
                        , dataMetadataMap.get(partitionDto.getDataUri())));
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
            , List<String> partitionIdsForDeletes, boolean checkIfExists) {
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
        SavePartitionResult savePartitionResult = splitManager.savePartitions(tableHandle, partitions, partitionIdsForDeletes, checkIfExists);

        // Save metadata
        log.info("Saving user metadata for partitions for {}", name);
        userMetadataService.saveMetadatas(session.getUser(), partitionDtos, true);
        // delete metadata
        if( !deletePartitions.isEmpty()) {
            log.info("Deleting user metadata for partitions with names {} for {}", partitionIdsForDeletes, name);
            userMetadataService.deleteMetadatas(deletePartitions, false);
        }

        result.setUpdated(savePartitionResult.getUpdated());
        result.setAdded(savePartitionResult.getAdded());

        return result;
    }

    private void validatePartitionName(String partitionName, List<String> partitionKeys) {
        for (String part : Splitter.on('/').omitEmptyStrings().split(partitionName)) {
            if (part.contains("=")) {
                String[] values = part.split("=", 2);

                if( partitionKeys == null || !partitionKeys.contains(values[0])){
                    throw new IllegalArgumentException(String.format("Partition name %s is invalid", partitionName));
                }
            } else {
                throw new IllegalArgumentException(String.format("Partition name %s is invalid", partitionName));
            }
        }
    }

    @Override
    public void delete(QualifiedName name, List<String> partitionIds) {
        TagList tags = BasicTagList.of("catalog", name.getCatalogName(), "database", name.getDatabaseName(), "table", name.getTableName());
        DynamicGauge.set(LogConstants.GaugeDeletePartitions.toString(), tags, partitionIds.size());
        Optional<TableHandle> tableHandle = tableService.getTableHandle(name);
        if (tableHandle.isPresent() && !partitionIds.isEmpty()) {
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
        List<QualifiedName> result = Lists.newArrayList();

        catalogService.getCatalogNames().stream().forEach(catalog -> {
            Session session = sessionProvider.getSession(QualifiedName.ofCatalog(catalog.getCatalogName()));
            List<SchemaTablePartitionName> schemaTablePartitionNames = splitManager.getPartitionNames( session, uri, prefixSearch);
            List<QualifiedName> qualifiedNames = schemaTablePartitionNames.stream().map(
                    schemaTablePartitionName -> QualifiedName.ofPartition( catalog.getConnectorName()
                            , schemaTablePartitionName.getTableName().getSchemaName()
                            , schemaTablePartitionName.getTableName().getTableName()
                            , schemaTablePartitionName.getPartitionId())).collect(Collectors.toList());
            result.addAll(qualifiedNames);
        });
        return result;
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
