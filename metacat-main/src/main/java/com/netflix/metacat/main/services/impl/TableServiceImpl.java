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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.exception.NotFoundException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePreEvent;
import com.netflix.metacat.common.server.connectors.ConnectorManager;
import com.netflix.metacat.common.server.services.DatabaseService;
import com.netflix.metacat.common.server.services.TableService;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
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
    private final ConnectorManager connectorManager;
    private final DatabaseService databaseService;
    private final TagService tagService;
    private final UserMetadataService userMetadataService;
    private final MetacatEventBus eventBus;
    private final ConverterUtil converterUtil;

    /**
     * Constructor.
     *
     * @param connectorManager     connector manager
     * @param databaseService      database service
     * @param tagService           tag service
     * @param userMetadataService  user metadata service
     * @param eventBus             Internal event bus
     * @param converterUtil        utility to convert to/from Dto to connector resources
     */
    public TableServiceImpl(
        final ConnectorManager connectorManager,
        final DatabaseService databaseService,
        final TagService tagService,
        final UserMetadataService userMetadataService,
        final MetacatEventBus eventBus,
        final ConverterUtil converterUtil
    ) {
        this.connectorManager = connectorManager;
        this.databaseService = databaseService;
        this.tagService = tagService;
        this.userMetadataService = userMetadataService;
        this.eventBus = eventBus;
        this.converterUtil = converterUtil;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto create(final QualifiedName name, final TableDto tableDto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        validate(name);
        //
        // Set the owner,if null, with the session user name.
        //
        setOwnerIfNull(tableDto, metacatRequestContext.getUserName());
        log.info("Creating table {}", name);
        eventBus.postSync(new MetacatCreateTablePreEvent(name, metacatRequestContext, this, tableDto));
        final ConnectorTableService service = connectorManager.getTableService(name.getCatalogName());
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        service.create(connectorRequestContext, converterUtil.fromTableDto(tableDto));

        if (tableDto.getDataMetadata() != null || tableDto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for table {}", name);
            userMetadataService.saveMetadata(metacatRequestContext.getUserName(), tableDto, false);
            tag(name, tableDto.getDefinitionMetadata());
        }
        final TableDto dto = get(name, true).orElseThrow(() -> new IllegalStateException("Should exist"));
        eventBus.postAsync(new MetacatCreateTablePostEvent(name, metacatRequestContext, this, dto));
        return dto;
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
                final Set<String> result = tagService.setTableTags(name, tags, false);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto deleteAndReturn(final QualifiedName name, final boolean isMView) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        eventBus.postSync(new MetacatDeleteTablePreEvent(name, metacatRequestContext, this));
        validate(name);
        final ConnectorTableService service = connectorManager.getTableService(name.getCatalogName());
        final Optional<TableDto> oTable = get(name, true);
        if (oTable.isPresent()) {
            log.info("Drop table {}", name);
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            service.delete(connectorRequestContext, name);
        }

        final TableDto tableDto = oTable.orElseGet(() -> {
            // If the table doesn't exist construct a blank copy we can use to delete the definitionMetadata and tags
            final TableDto t = new TableDto();
            t.setName(name);
            return t;
        });

        // Delete the metadata.  Type doesn't matter since we discard the result
        log.info("Deleting user metadata for table {}", name);
        userMetadataService.deleteMetadatas(metacatRequestContext.getUserName(), Lists.newArrayList(tableDto));
        log.info("Deleting tags for table {}", name);
        tagService.delete(name, false);
        eventBus.postAsync(new MetacatDeleteTablePostEvent(name, metacatRequestContext, this, tableDto));
        return tableDto;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<TableDto> get(final QualifiedName name, final boolean includeUserMetadata) {
        return get(name, true, includeUserMetadata, includeUserMetadata);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<TableDto> get(final QualifiedName name, final boolean includeInfo,
                                  final boolean includeDefinitionMetadata, final boolean includeDataMetadata) {
        validate(name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        final ConnectorTableService service = connectorManager.getTableService(name.getCatalogName());
        final TableDto table;
        if (includeInfo) {
            try {
                table = converterUtil.toTableDto(service.get(connectorRequestContext, name));
            } catch (NotFoundException ignored) {
                return Optional.empty();
            }
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
            TableDto dto = table;
            if (!includeInfo) {
                try {
                    dto = converterUtil.toTableDto(service.get(connectorRequestContext, name));
                } catch (NotFoundException ignored) {
                }
            }
            if (dto != null && dto.getSerde() != null) {
                final Optional<ObjectNode> dataMetadata =
                    userMetadataService.getDataMetadata(dto.getSerde().getUri());
                if (dataMetadata.isPresent()) {
                    table.setDataMetadata(dataMetadata.get());
                }
            }
        }

        return Optional.of(table);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rename(
        final QualifiedName oldName,
        final QualifiedName newName,
        final boolean isMView
    ) {
        validate(oldName);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorTableService service = connectorManager.getTableService(oldName.getCatalogName());

        final TableDto oldTable = get(oldName, true).orElseThrow(() -> new TableNotFoundException(oldName));
        if (oldTable != null) {
            //Ignore if the operation is not supported, so that we can at least go ahead and save the user metadata
            eventBus.postSync(new MetacatRenameTablePreEvent(oldName, metacatRequestContext, this, newName));
            try {
                log.info("Renaming {} {} to {}", isMView ? "view" : "table", oldName, newName);
                final ConnectorRequestContext connectorRequestContext
                    = converterUtil.toConnectorContext(metacatRequestContext);
                service.rename(connectorRequestContext, oldName, newName);
            } catch (UnsupportedOperationException ignored) {
            }
            userMetadataService.renameDefinitionMetadataKey(oldName, newName);
            tagService.rename(oldName, newName.getTableName());

            final TableDto dto = get(newName, true).orElseThrow(() -> new IllegalStateException("should exist"));
            eventBus.postAsync(new MetacatRenameTablePostEvent(oldName, metacatRequestContext, this, oldTable, dto));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(final QualifiedName name, final TableDto tableDto) {
        updateAndReturn(name, tableDto);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto updateAndReturn(final QualifiedName name, final TableDto tableDto) {
        validate(name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorTableService service = connectorManager.getTableService(name.getCatalogName());
        final TableDto oldTable = get(name, true).orElseThrow(() -> new TableNotFoundException(name));
        eventBus.postSync(new MetacatUpdateTablePreEvent(name, metacatRequestContext, this, oldTable, tableDto));
        //Ignore if the operation is not supported, so that we can at least go ahead and save the user metadata
        if (isTableInfoProvided(tableDto)) {
            try {
                log.info("Updating table {}", name);
                final ConnectorRequestContext connectorRequestContext
                    = converterUtil.toConnectorContext(metacatRequestContext);
                service.update(connectorRequestContext, converterUtil.fromTableDto(tableDto));
            } catch (UnsupportedOperationException ignored) {
            }
        }

        // Merge in metadata if the user sent any
        if (tableDto.getDataMetadata() != null || tableDto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for table {}", name);
            userMetadataService.saveMetadata(metacatRequestContext.getUserName(), tableDto, true);
        }
        final TableDto updatedDto = get(name, true).orElseThrow(() -> new IllegalStateException("should exist"));
        eventBus.postAsync(new MetacatUpdateTablePostEvent(name, metacatRequestContext, this, oldTable, updatedDto));
        return updatedDto;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(final QualifiedName name) {
        deleteAndReturn(name, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto get(final QualifiedName name) {
        final Optional<TableDto> dto = get(name, true);
        return dto.orElse(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto copy(final QualifiedName sourceName, final QualifiedName targetName) {
        // Source should be same
        if (!sourceName.getCatalogName().equals(targetName.getCatalogName())) {
            throw new MetacatNotSupportedException("Cannot copy a table from a different source");
        }
        // Error out when source table does not exists
        final Optional<TableDto> oTable = get(sourceName, false);
        if (!oTable.isPresent()) {
            throw new TableNotFoundException(sourceName);
        }
        // Error out when target table already exists
        final Optional<TableDto> oTargetTable = get(targetName, false);
        if (oTargetTable.isPresent()) {
            throw new TableNotFoundException(targetName);
        }
        return copy(oTable.get(), targetName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto copy(final TableDto tableDto, final QualifiedName targetName) {
        final QualifiedName databaseName =
            QualifiedName.ofDatabase(targetName.getCatalogName(), targetName.getDatabaseName());
        if (!databaseService.exists(databaseName)) {
            final DatabaseDto databaseDto = new DatabaseDto();
            databaseDto.setName(databaseName);
            databaseService.create(databaseName, databaseDto);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveMetadata(final QualifiedName name, final ObjectNode definitionMetadata,
                             final ObjectNode dataMetadata) {
        validate(name);
        final Optional<TableDto> tableDtoOptional = get(name, false);
        if (tableDtoOptional.isPresent()) {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final TableDto tableDto = tableDtoOptional.get();
            tableDto.setDefinitionMetadata(definitionMetadata);
            tableDto.setDataMetadata(dataMetadata);
            log.info("Saving user metadata for table {}", name);
            userMetadataService.saveMetadata(metacatRequestContext.getUserName(), tableDto, true);
            tag(name, tableDto.getDefinitionMetadata());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> getQualifiedNames(final String uri, final boolean prefixSearch) {
        final List<QualifiedName> result = Lists.newArrayList();

        connectorManager.getCatalogs().keySet().forEach(catalogName -> {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorTableService service = connectorManager.getTableService(catalogName);
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            try {
                final Map<String, List<QualifiedName>> names =
                    service.getTableNames(connectorRequestContext, Lists.newArrayList(uri), prefixSearch);
                final List<QualifiedName> qualifiedNames = names.values().stream().flatMap(Collection::stream)
                    .collect(Collectors.toList());
                result.addAll(qualifiedNames);
            } catch (final UnsupportedOperationException uoe) {
                log.debug("Catalog {} doesn't support getting table names by URI. Skipping", catalogName);
            }
        });
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<QualifiedName>> getQualifiedNames(final List<String> uris, final boolean prefixSearch) {
        final Map<String, List<QualifiedName>> result = Maps.newHashMap();

        connectorManager.getCatalogs().keySet().forEach(catalogName -> {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final ConnectorTableService service = connectorManager.getTableService(catalogName);
            final ConnectorRequestContext connectorRequestContext
                = converterUtil.toConnectorContext(metacatRequestContext);
            try {
                final Map<String, List<QualifiedName>> names =
                    service.getTableNames(connectorRequestContext, uris, prefixSearch);
                names.forEach((uri, qNames) -> {
                    final List<QualifiedName> existingNames = result.get(uri);
                    if (existingNames == null) {
                        result.put(uri, qNames);
                    } else {
                        existingNames.addAll(qNames);
                    }
                });
            } catch (final UnsupportedOperationException uoe) {
                log.debug("Catalog {} doesn't support getting table names by URI. Skipping", catalogName);
            }
        });
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final QualifiedName name) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final ConnectorTableService service = connectorManager.getTableService(name.getCatalogName());
        final ConnectorRequestContext connectorRequestContext = converterUtil.toConnectorContext(metacatRequestContext);
        return service.exists(connectorRequestContext, name);
    }

    private void validate(final QualifiedName name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkArgument(name.isTableDefinition(), "Definition {} does not refer to a table", name);
    }
}
