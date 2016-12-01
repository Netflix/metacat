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
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.AuditInfo;
import com.facebook.presto.spi.ConnectorTableDetailMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Sort;
import com.facebook.presto.spi.TableNotFoundException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.converters.PrestoConverters;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.SessionProvider;
import com.netflix.metacat.main.services.TableService;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Created by amajumdar on 4/13/15.
 */
public class MViewServiceImpl implements MViewService {
    private static final Logger log = LoggerFactory.getLogger(MViewServiceImpl.class);
    public static final String VIEW_DB_NAME = "franklinviews";
    private static final List<String> SUPPORTED_SOURCES = Lists.newArrayList("hive", "s3", "aegisthus");
    @Inject
    MetadataManager metadataManager;
    @Inject
    SessionProvider sessionProvider;
    @Inject
    TableService tableService;
    @Inject
    PartitionService partitionService;
    @Inject
    PrestoConverters prestoConverters;
    @Inject
    UserMetadataService userMetadataService;

    /**
     * Creates the materialized view using the schema of the give table
     * Assumes that the "franklinviews" database name already exists in the given catalog.
     */
    @Override
    public TableDto create(
        @Nonnull
            QualifiedName name) {
        TableDto result = null;
        // Get the table
        log.info("Get the table {}", name);
        Optional<TableDto> oTable = tableService.get(name, false);
        if (oTable.isPresent()) {
            TableDto table = oTable.get();
            String viewName = createViewName(name);
            QualifiedName targetName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, viewName);
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
        } else {
            throw new TableNotFoundException(new SchemaTableName(name.getDatabaseName(), name.getTableName()));
        }
        return result;
    }

    @Override
    public void create(
        @Nonnull
            QualifiedName name,
        @Nonnull
            TableDto dto) {
        // Ignore the dto passed
        create(name);
    }

    @Override
    public TableDto deleteAndReturn(
        @Nonnull
            QualifiedName name) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        log.info("Deleting view {}.", viewQName);
        return tableService.deleteAndReturn(viewQName, true);
    }

    @Override
    public void delete(
        @Nonnull
            QualifiedName name) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        tableService.deleteAndReturn(viewQName, true);
    }

    @Override
    public void update(
        @Nonnull
            QualifiedName name,
        @Nonnull
            TableDto tableDto) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        log.info("Updating view {}.", viewQName);
        tableService.update(viewQName, tableDto);
    }

    @Override
    public TableDto get(
        @Nonnull
            QualifiedName name) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return tableService.get(viewQName);
    }

    @Override
    public Optional<TableDto> getOpt(
        @Nonnull
            QualifiedName name) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        Optional<TableDto> result = tableService.get(viewQName, false);

        //
        // User definition metadata of the underlying table is returned
        //
        if (result.isPresent()) {
            TableDto table = result.get();
            table.setName(name);
            QualifiedName tableName = QualifiedName
                .ofTable(name.getCatalogName(), name.getDatabaseName(), name.getTableName());
            Optional<ObjectNode> definitionMetadata = userMetadataService.getDefinitionMetadata(tableName);
            if (definitionMetadata.isPresent()) {
                userMetadataService.populateMetadata(table, definitionMetadata.get(), null);
            }
        }
        return result;
    }

    @Override
    public void snapshotPartitions(
        @Nonnull
            QualifiedName name, String filter) {
        List<PartitionDto> partitionDtos = partitionService.list(name, filter, null, null, null, false, false, true);
        if (partitionDtos != null && !partitionDtos.isEmpty()) {
            log.info("Snapshot partitions({}) for view {}.", partitionDtos.size(), name);
            savePartitions(name, partitionDtos, null, false, true, false);
        }
    }

    @Override
    public PartitionsSaveResponseDto savePartitions(
        @Nonnull
            QualifiedName name, List<PartitionDto> partitionDtos
        , List<String> partitionIdsForDeletes, boolean merge, boolean checkIfExists, boolean alterIfExists) {
        if (partitionDtos == null || partitionDtos.isEmpty()) {
            return new PartitionsSaveResponseDto();
        }
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        partitionDtos.forEach(partitionDto ->
            partitionDto.setName(QualifiedName
                .ofPartition(viewQName.getCatalogName(), viewQName.getDatabaseName(), viewQName.getTableName(),
                    partitionDto.getName().getPartitionName())));
        if (merge) {
            List<String> partitionNames = partitionDtos.stream().map(
                partitionDto -> partitionDto.getName().getPartitionName()).collect(Collectors.toList());
            List<PartitionDto> existingPartitions = partitionService
                .list(viewQName, null, partitionNames, null, null, false, false, false);
            Map<String, PartitionDto> existingPartitionsMap = existingPartitions.stream()
                .collect(Collectors
                    .toMap(partitionDto -> partitionDto.getName().getPartitionName(), Function.identity()));
            List<PartitionDto> mergedPartitions = partitionDtos.stream()
                .map(partitionDto -> {
                    String partitionName = partitionDto.getName().getPartitionName();
                    PartitionDto existingPartition = existingPartitionsMap.get(partitionName);
                    return mergePartition(partitionDto, existingPartition);
                }).collect(Collectors.toList());
            return partitionService
                .save(viewQName, mergedPartitions, partitionIdsForDeletes, checkIfExists, alterIfExists);
        } else {
            return partitionService
                .save(viewQName, partitionDtos, partitionIdsForDeletes, checkIfExists, alterIfExists);
        }
    }

    private PartitionDto mergePartition(PartitionDto partitionDto, PartitionDto existingPartition) {
        if (existingPartition != null) {
            StorageDto existingSerde = existingPartition.getSerde();
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
    public void deletePartitions(
        @Nonnull
            QualifiedName name, List<String> partitionIds) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        partitionService.delete(viewQName, partitionIds);
    }

    @Override
    public List<PartitionDto> listPartitions(
        @Nonnull
            QualifiedName name, String filter, List<String> partitionNames, Sort sort,
        Pageable pageable, boolean includeUserMetadata, boolean includePartitionDetails) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return partitionService
            .list(viewQName, filter, partitionNames, sort, pageable, includeUserMetadata, includeUserMetadata,
                includePartitionDetails);
    }

    @Override
    public List<String> getPartitionKeys(QualifiedName name, String filter, List<String> partitionNames, Sort sort,
        Pageable pageable) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return partitionService.getPartitionKeys(viewQName, filter, partitionNames, sort, pageable);
    }

    @Override
    public List<String> getPartitionUris(QualifiedName name, String filter, List<String> partitionNames, Sort sort,
        Pageable pageable) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return partitionService.getPartitionUris(viewQName, filter, partitionNames, sort, pageable);
    }

    @Override
    public Integer partitionCount(QualifiedName name) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return partitionService.count(viewQName);
    }

    @Override
    public List<NameDateDto> list(
        @Nonnull
            QualifiedName name) {
        QualifiedName viewDbName = QualifiedName.ofDatabase(name.getCatalogName(), VIEW_DB_NAME);
        QualifiedTablePrefix viewDbNamePrefix = new QualifiedTablePrefix(name.getCatalogName(), VIEW_DB_NAME);
        // Get the session
        Session session = sessionProvider.getSession(viewDbName);
        List<QualifiedTableName> tableNames = Lists.newArrayList();
        try {
            tableNames = metadataManager.listTables(session, viewDbNamePrefix);
        } catch (Exception ignored) {
            // ignore. Return an empty list if database 'franklinviews' does not exist
        }
        if (!name.isDatabaseDefinition() && name.isCatalogDefinition()) {
            return tableNames.stream()
                .map(prestoConverters::toQualifiedName)
                .map(viewName -> {
                    NameDateDto dto = new NameDateDto();
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
                    NameDateDto dto = new NameDateDto();
                    dto.setName(QualifiedName
                        .ofView(qualifiedTableName.getCatalogName(), name.getDatabaseName(), name.getTableName(),
                            qualifiedTableName.getTableName().substring(prefix.length())));
                    return dto;
                })
                .collect(Collectors.toList());
        }
    }

    @Override
    public void saveMetadata(
        @Nonnull
            QualifiedName name, ObjectNode definitionMetadata, ObjectNode dataMetadata) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        tableService.saveMetadata(viewQName, definitionMetadata, dataMetadata);
    }

    @Override
    public void rename(QualifiedName name, QualifiedName newViewName) {
        QualifiedName oldViewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        QualifiedName newViewQName = QualifiedName
            .ofTable(newViewName.getCatalogName(), VIEW_DB_NAME, createViewName(newViewName));
        tableService.rename(oldViewQName, newViewQName, true);
    }

    @Override
    public boolean exists(
        @Nonnull
            QualifiedName name) {
        QualifiedName viewQName = QualifiedName.ofTable(name.getCatalogName(), VIEW_DB_NAME, createViewName(name));
        return tableService.exists(viewQName);
    }

    /**
     * Validate the qualified name.
     * Validate if the catalog is one of the catalogs that support views.
     * Assumes that the "franklinviews" database name already exists in the given catalog.
     */
    private Session validateAndGetSession(QualifiedName name) {
        checkNotNull(name, "name cannot be null");
        checkState(name.isViewDefinition(), "name %s is not for a view", name);

        if (!Iterables.contains(SUPPORTED_SOURCES, name.getCatalogName())) {
            throw new MetacatNotSupportedException(
                String.format("This catalog (%s) doesn't support views", name.getCatalogName()));
        }
        return sessionProvider.getSession(name);
    }

    /**
     * The view is going to be represented by a table in a special db in Franklin.  As such there must be
     * a conversion from view id -> view table id like so:
     * [dbName]_[tableName]_[viewName]
     */
    private String createViewName(QualifiedName name) {
        return String.format("%s_%s_%s", name.getDatabaseName(), name.getTableName(), name.getViewName());
    }
}
