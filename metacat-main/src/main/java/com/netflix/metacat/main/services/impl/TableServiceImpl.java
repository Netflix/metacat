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
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ConnectorTableDetailMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.StorageInfo;
import com.facebook.presto.spi.TableNotFoundException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.usermetadata.TagService;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.common.util.ThreadServiceManager;
import com.netflix.metacat.converters.PrestoConverters;
import com.netflix.metacat.main.connector.MetacatConnectorManager;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.SessionProvider;
import com.netflix.metacat.main.services.TableService;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Table service implementation.
 */
@Slf4j
public class TableServiceImpl implements TableService {
    private static final String NAME_TAGS = "tags";
    @Inject
    private MetacatConnectorManager metacatConnectorManager;
    @Inject
    private MetadataManager metadataManager;
    @Inject
    private PrestoConverters prestoConverters;
    @Inject
    private SessionProvider sessionProvider;
    @Inject
    private UserMetadataService userMetadataService;
    @Inject
    private DatabaseService databaseService;
    @Inject
    private TagService tagService;
    @Inject
    private MViewService mViewService;
    @Inject
    private PartitionService partitionService;
    @Inject
    private ThreadServiceManager threadServiceManager;
    @Inject
    private Config config;

    @Override
    public void create(
        @Nonnull
        final QualifiedName name,
        @Nonnull
        final TableDto tableDto) {
        final Session session = validateAndGetSession(name);
        //
        // Set the owner,if null, with the session user name.
        //
        setOwnerIfNull(tableDto, session.getUser());
        log.info("Creating table {}", name);
        metadataManager.createTable(
            session,
            name.getCatalogName(),
            prestoConverters.fromTableDto(name, tableDto, metadataManager.getTypeManager())
        );

        if (tableDto.getDataMetadata() != null || tableDto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for table {}", name);
            userMetadataService.saveMetadata(session.getUser(), tableDto, false);
            tag(name, tableDto.getDefinitionMetadata());
        }
    }

    private void setOwnerIfNull(final TableDto tableDto, final String user) {
        if (!Strings.isNullOrEmpty(user)) {
            StorageDto serde = tableDto.getSerde();
            if (serde == null) {
                serde = new StorageDto();
                tableDto.setSerde(serde);
            }
            if (Strings.isNullOrEmpty(serde.getOwner())) {
                serde.setOwner(user);
            }
        }
    }

    private void tag(final QualifiedName name, final ObjectNode definitionMetadata) {
        if (definitionMetadata != null && definitionMetadata.get(NAME_TAGS) != null) {
            final JsonNode tagsNode = definitionMetadata.get(NAME_TAGS);
            final Set<String> tags = Sets.newHashSet();
            if (tagsNode.isArray() && tagsNode.size() > 0) {
                for (JsonNode tagNode : tagsNode) {
                    tags.add(tagNode.textValue());
                }
                log.info("Setting tags {} for table {}", tags, name);
                tagService.setTableTags(name, tags, false);
            }
        }
    }

    @Override
    public TableDto deleteAndReturn(
        @Nonnull
        final QualifiedName name, final boolean isMView) {
        final Session session = validateAndGetSession(name);
        final QualifiedTableName tableName = prestoConverters.getQualifiedTableName(name);

        final Optional<TableHandle> tableHandle = metadataManager.getTableHandle(session, tableName);
        final Optional<TableDto> oTable = get(name, true);
        if (oTable.isPresent()) {
            log.info("Drop table {}", name);
            metadataManager.dropTable(session, tableHandle.get());
        }

        final TableDto tableDto = oTable.orElseGet(() -> {
            // If the table doesn't exist construct a blank copy we can use to delete the definitionMetadata and tags
            final TableDto t = new TableDto();
            t.setName(name);
            return t;
        });

        // Delete the metadata.  Type doesn't matter since we discard the result
        log.info("Deleting user metadata for table {}", name);
        userMetadataService.deleteMetadatas(session.getUser(), Lists.newArrayList(tableDto));
        log.info("Deleting tags for table {}", name);
        tagService.delete(name, false);
        if (config.canCascadeViewsMetadataOnTableDelete() && !isMView) {
            // Spawning off since this is a time consuming task
            threadServiceManager.getExecutor().submit(() -> {
                try {
                    // delete views associated with this table
                    final List<NameDateDto> viewNames = mViewService.list(name);
                    viewNames.forEach(viewName -> mViewService.deleteAndReturn(viewName.getName()));
                } catch (Exception e) {
                    log.warn("Failed cleaning mviews after deleting table {}", name);
                }
                // delete table partitions metadata
                try {
                    final List<QualifiedName> names = userMetadataService.getDescendantDefinitionNames(name);
                    if (names != null && !names.isEmpty()) {
                        userMetadataService.deleteDefinitionMetadatas(names);
                    }
                } catch (Exception e) {
                    log.warn("Failed cleaning partition definition metadata after deleting table {}", name);
                }
            });
        }
        return tableDto;
    }

    @Override
    public Optional<TableDto> get(
        @Nonnull
        final QualifiedName name, final boolean includeUserMetadata) {
        return get(name, true, includeUserMetadata, includeUserMetadata);
    }

    @Override
    public Optional<TableDto> get(
        @Nonnull
        final QualifiedName name, final boolean includeInfo, final boolean includeDefinitionMetadata,
        final boolean includeDataMetadata) {
        final Session session = validateAndGetSession(name);
        final TableDto table;
        Optional<TableMetadata> tableMetadata = Optional.empty();
        if (includeInfo) {
            try {
                tableMetadata = Optional.ofNullable(getTableMetadata(name, session));
            } catch (NotFoundException ignored) {
                return Optional.empty();
            }
            if (!tableMetadata.isPresent()) {
                return Optional.empty();
            }
            final String type = metacatConnectorManager.getCatalogConfig(name).getType();
            table = prestoConverters.toTableDto(name, type, tableMetadata.get());
        } else {
            table = new TableDto();
            table.setName(name);
        }

        if (includeDefinitionMetadata) {
            final Optional<ObjectNode> definitionMetadata = userMetadataService.getDefinitionMetadata(name);
            if (definitionMetadata.isPresent()) {
                table.setDefinitionMetadata(definitionMetadata.get());
            }
        }

        if (includeDataMetadata) {
            if (!tableMetadata.isPresent()) {
                tableMetadata = Optional.ofNullable(getTableMetadata(name, session));
            }
            if (tableMetadata.isPresent()) {
                final ConnectorTableMetadata connectorTableMetadata = tableMetadata.get().getMetadata();
                if (connectorTableMetadata instanceof ConnectorTableDetailMetadata) {
                    final ConnectorTableDetailMetadata detailMetadata =
                        (ConnectorTableDetailMetadata) connectorTableMetadata;
                    final StorageInfo storageInfo = detailMetadata.getStorageInfo();
                    if (storageInfo != null) {
                        final Optional<ObjectNode> dataMetadata =
                            userMetadataService.getDataMetadata(storageInfo.getUri());
                        if (dataMetadata.isPresent()) {
                            table.setDataMetadata(dataMetadata.get());
                        }
                    }
                }
            }
        }

        return Optional.of(table);
    }

    @Override
    public Optional<TableHandle> getTableHandle(
        @Nonnull
        final QualifiedName name) {
        final Session session = validateAndGetSession(name);

        final QualifiedTableName qualifiedTableName = prestoConverters.getQualifiedTableName(name);
        return metadataManager.getTableHandle(session, qualifiedTableName);
    }

    private TableMetadata getTableMetadata(final QualifiedName name, final Optional<TableHandle> tableHandle) {
        if (!tableHandle.isPresent()) {
            return null;
        }
        final Session session = validateAndGetSession(name);
        final TableMetadata result = metadataManager.getTableMetadata(session, tableHandle.get());
        Preconditions.
            checkState(name.getDatabaseName().equals(result.getTable().getSchemaName()), "Unexpected database");
        Preconditions.checkState(name.getTableName().equals(result.getTable().getTableName()), "Unexpected table");

        return result;
    }

    private TableMetadata getTableMetadata(final QualifiedName name, final Session session) {
        final QualifiedTableName qualifiedTableName = prestoConverters.getQualifiedTableName(name);
        final Optional<TableHandle> tableHandle = metadataManager.getTableHandle(session, qualifiedTableName);
        return getTableMetadata(name, tableHandle);
    }

    @Override
    public void rename(
        @Nonnull
        final QualifiedName oldName,
        @Nonnull
        final QualifiedName newName, final boolean isMView) {
        final Session session = validateAndGetSession(oldName);

        final QualifiedTableName oldPrestoName = prestoConverters.getQualifiedTableName(oldName);
        final QualifiedTableName newPrestoName = prestoConverters.getQualifiedTableName(newName);

        final Optional<TableHandle> tableHandle = metadataManager.getTableHandle(session, oldPrestoName);
        if (tableHandle.isPresent()) {
            //Ignore if the operation is not supported, so that we can at least go ahead and save the user metadata
            try {
                log.info("Renaming {} {} to {}", isMView ? "view" : "table", oldName, newName);
                metadataManager.renameTable(session, tableHandle.get(), newPrestoName);

                if (!isMView) {
                    final String prefix = String.format("%s_%s_", oldName.getDatabaseName(),
                        MoreObjects.firstNonNull(oldName.getTableName(), ""));
                    final List<NameDateDto> views = mViewService.list(oldName);
                    if (views != null && !views.isEmpty()) {
                        views.forEach(view -> {
                            final QualifiedName newViewName = QualifiedName
                                .ofView(oldName.getCatalogName(), oldName.getDatabaseName(), newName.getTableName(),
                                    view.getName().getViewName());
                            mViewService.rename(view.getName(), newViewName);
                        });
                    }
                }
            } catch (PrestoException e) {
                if (!StandardErrorCode.NOT_SUPPORTED.toErrorCode().equals(e.getErrorCode())) {
                    throw e;
                }
            }
            userMetadataService.renameDefinitionMetadataKey(oldName, newName);
            tagService.rename(oldName, newName.getTableName());
        }
    }

    @Override
    public void update(
        @Nonnull
        final QualifiedName name,
        @Nonnull
        final TableDto tableDto) {
        final Session session = validateAndGetSession(name);

        //Ignore if the operation is not supported, so that we can at least go ahead and save the user metadata
        if (isTableInfoProvided(tableDto)) {
            try {
                log.info("Updating table {}", name);
                metadataManager
                    .alterTable(session,
                        prestoConverters.fromTableDto(name, tableDto, metadataManager.getTypeManager()));
            } catch (PrestoException e) {
                if (!StandardErrorCode.NOT_SUPPORTED.toErrorCode().equals(e.getErrorCode())) {
                    throw e;
                }
            }
        }

        // Merge in metadata if the user sent any
        if (tableDto.getDataMetadata() != null || tableDto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for table {}", name);
            userMetadataService.saveMetadata(session.getUser(), tableDto, true);
        }
    }

    private boolean isTableInfoProvided(final TableDto tableDto) {
        boolean result = false;
        if ((tableDto.getFields() != null && !tableDto.getFields().isEmpty())
            || tableDto.getSerde() != null
            || (tableDto.getMetadata() != null && !tableDto.getMetadata().isEmpty())
            || tableDto.getAudit() != null) {
            result = true;
        }
        return result;
    }

    @Override
    public void delete(
        @Nonnull
        final QualifiedName name) {
        deleteAndReturn(name, false);
    }

    @Override
    public TableDto get(
        @Nonnull
        final QualifiedName name) {
        final Optional<TableDto> dto = get(name, true);
        return dto.orElse(null);
    }

    @Override
    public TableDto copy(
        @Nonnull
        final QualifiedName sourceName,
        @Nonnull
        final QualifiedName targetName) {
        // Source should be same
        if (!sourceName.getCatalogName().equals(targetName.getCatalogName())) {
            throw new MetacatNotSupportedException("Cannot copy a table from a different source");
        }
        // Error out when source table does not exists
        final Optional<TableDto> oTable = get(sourceName, false);
        if (!oTable.isPresent()) {
            throw new TableNotFoundException(
                new SchemaTableName(sourceName.getDatabaseName(), sourceName.getTableName()));
        }
        // Error out when target table already exists
        final Optional<TableDto> oTargetTable = get(targetName, false);
        if (oTargetTable.isPresent()) {
            throw new TableNotFoundException(
                new SchemaTableName(targetName.getDatabaseName(), targetName.getTableName()));
        }
        return copy(oTable.get(), targetName);
    }

    @Override
    public TableDto copy(
        @Nonnull
        final TableDto tableDto,
        @Nonnull
        final QualifiedName targetName) {
        if (!databaseService.exists(targetName)) {
            databaseService.create(targetName, new DatabaseDto());
        }
        final TableDto targetTableDto = new TableDto();
        targetTableDto.setName(targetName);
        targetTableDto.setFields(tableDto.getFields());
        targetTableDto.setPartition_keys(tableDto.getPartition_keys());
        final StorageDto storageDto = tableDto.getSerde();
        if (storageDto != null) {
            final StorageDto targetStorageDto = new StorageDto();
            targetStorageDto.setInputFormat(storageDto.getInputFormat());
            targetStorageDto.setOwner(storageDto.getOwner());
            targetStorageDto.setOutputFormat(storageDto.getOutputFormat());
            targetStorageDto.setParameters(storageDto.getParameters());
            targetStorageDto.setUri(storageDto.getUri());
            targetStorageDto.setSerializationLib(storageDto.getSerializationLib());
            targetTableDto.setSerde(targetStorageDto);
        }
        create(targetName, targetTableDto);
        return targetTableDto;
    }

    @Override
    public void saveMetadata(
        @Nonnull
        final QualifiedName name, final ObjectNode definitionMetadata, final ObjectNode dataMetadata) {
        final Session session = validateAndGetSession(name);
        final Optional<TableDto> tableDtoOptional = get(name, false);
        if (tableDtoOptional.isPresent()) {
            final TableDto tableDto = tableDtoOptional.get();
            tableDto.setDefinitionMetadata(definitionMetadata);
            tableDto.setDataMetadata(dataMetadata);
            log.info("Saving user metadata for table {}", name);
            userMetadataService.saveMetadata(session.getUser(), tableDto, true);
            tag(name, tableDto.getDefinitionMetadata());
        }
    }

    @Override
    public List<QualifiedName> getQualifiedNames(final String uri, final boolean prefixSearch) {
        final List<QualifiedName> result = Lists.newArrayList();
        final Map<String, String> catalogNames = metadataManager.getCatalogNames();

        catalogNames.values().forEach(catalogName -> {
            final Session session = sessionProvider.getSession(QualifiedName.ofCatalog(catalogName));
            final List<SchemaTableName> schemaTableNames = metadataManager.getTableNames(session, uri, prefixSearch);
            final List<QualifiedName> qualifiedNames = schemaTableNames.stream().map(
                schemaTableName -> QualifiedName
                    .ofTable(catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName()))
                .collect(Collectors.toList());
            result.addAll(qualifiedNames);
        });
        return result;
    }

    @Override
    public Map<String, List<QualifiedName>> getQualifiedNames(final List<String> uris, final boolean prefixSearch) {
        final Map<String, List<QualifiedName>> result = Maps.newHashMap();
        final Map<String, String> catalogNames = metadataManager.getCatalogNames();

        catalogNames.values().forEach(catalogName -> {
            final Session session = sessionProvider.getSession(QualifiedName.ofCatalog(catalogName));
            final Map<String, List<SchemaTableName>> schemaTableNames =
                metadataManager.getTableNames(session, uris, prefixSearch);
            schemaTableNames.forEach((uri, schemaTableNames1) -> {
                final List<QualifiedName> names = schemaTableNames1.stream().map(
                    schemaTableName -> QualifiedName
                        .ofTable(catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName()))
                    .collect(Collectors.toList());
                final List<QualifiedName> existingNames = result.get(uri);
                if (existingNames == null) {
                    result.put(uri, names);
                } else {
                    existingNames.addAll(names);
                }
            });
        });
        return result;
    }

    @Override
    public boolean exists(
        @Nonnull
        final QualifiedName name) {
        return get(name, true, false, false).isPresent();
    }

    private Session validateAndGetSession(final QualifiedName name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkArgument(name.isTableDefinition(), "Definition {} does not refer to a table", name);

        return sessionProvider.getSession(name);
    }
}
