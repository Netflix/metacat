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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
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
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.AuthorizationService;
import com.netflix.metacat.common.server.usermetadata.GetMetadataInterceptorParameters;
import com.netflix.metacat.common.server.usermetadata.MetacatOperation;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.GetTableNamesServiceParameters;
import com.netflix.metacat.main.services.GetTableServiceParameters;
import com.netflix.metacat.main.services.TableService;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Table service implementation.
 */
@Slf4j
public class TableServiceImpl implements TableService {
    private static final String NAME_TAGS = "tags";
    private final DatabaseService databaseService;
    private final TagService tagService;
    private final UserMetadataService userMetadataService;
    private final MetacatEventBus eventBus;
    private final Registry registry;
    private final Config config;
    private final ConverterUtil converterUtil;
    private final ConnectorTableServiceProxy connectorTableServiceProxy;
    private final AuthorizationService authorizationService;

    /**
     * Constructor.
     *
     * @param connectorTableServiceProxy connector table service proxy
     * @param databaseService            database service
     * @param tagService                 tag service
     * @param userMetadataService        user metadata service
     * @param eventBus                   Internal event bus
     * @param registry                   registry handle
     * @param config                     configurations
     * @param converterUtil              utility to convert to/from Dto to connector resources
     * @param authorizationService       authorization service
     */
    public TableServiceImpl(
        final ConnectorTableServiceProxy connectorTableServiceProxy,
        final DatabaseService databaseService,
        final TagService tagService,
        final UserMetadataService userMetadataService,
        final MetacatEventBus eventBus,
        final Registry registry,
        final Config config,
        final ConverterUtil converterUtil,
        final AuthorizationService authorizationService
    ) {
        this.connectorTableServiceProxy = connectorTableServiceProxy;
        this.databaseService = databaseService;
        this.tagService = tagService;
        this.userMetadataService = userMetadataService;
        this.eventBus = eventBus;
        this.registry = registry;
        this.config = config;
        this.authorizationService = authorizationService;
        this.converterUtil = converterUtil;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto create(final QualifiedName name, final TableDto tableDto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        validate(name);
        this.authorizationService.checkPermission(metacatRequestContext.getUserName(),
            tableDto.getName(), MetacatOperation.CREATE);
        //
        // Set the owner,if null, with the session user name.
        //
        setOwnerIfNull(tableDto, metacatRequestContext.getUserName());
        log.info("Creating table {}", name);
        eventBus.post(new MetacatCreateTablePreEvent(name, metacatRequestContext, this, tableDto));
        connectorTableServiceProxy.create(name, converterUtil.fromTableDto(tableDto));

        if (tableDto.getDataMetadata() != null || tableDto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for table {}", name);
            final long start = registry.clock().wallTime();
            userMetadataService.saveMetadata(metacatRequestContext.getUserName(), tableDto, true);
            final long duration = registry.clock().wallTime() - start;
            log.info("Time taken to save user metadata for table {} is {} ms", name, duration);
            registry.timer(registry.createId(Metrics.TimerSaveTableMetadata.getMetricName()).withTags(name.parts()))
                .record(duration, TimeUnit.MILLISECONDS);
            tag(name, tableDto.getDefinitionMetadata());
        }
        final TableDto dto = get(name, GetTableServiceParameters.builder()
            .disableOnReadMetadataIntercetor(false)
            .includeInfo(true)
            .includeDataMetadata(true)
            .includeDefinitionMetadata(true)
            .build()).orElseThrow(() -> new IllegalStateException("Should exist"));
        eventBus.post(new MetacatCreateTablePostEvent(name, metacatRequestContext, this, dto));
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
                final Set<String> result = tagService.setTags(name, tags, false);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto deleteAndReturn(final QualifiedName name, final boolean isMView) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        validate(name);
        this.authorizationService.checkPermission(metacatRequestContext.getUserName(),
            name, MetacatOperation.DELETE);

        eventBus.post(new MetacatDeleteTablePreEvent(name, metacatRequestContext, this));

        TableDto tableDto = new TableDto();
        tableDto.setName(name);

        try {
            final Optional<TableDto> oTable = get(name,
                GetTableServiceParameters.builder()
                    .includeInfo(true)
                    .disableOnReadMetadataIntercetor(false)
                    .includeDefinitionMetadata(true)
                    .includeDataMetadata(true)
                    .build());
            tableDto = oTable.orElse(tableDto);
        } catch (Exception e) {
            handleException(name, true, "deleteAndReturn_get", e);
        }

        // Try to delete the table even if get above fails
        try {
            connectorTableServiceProxy.delete(name);
        } catch (NotFoundException ignored) {
            log.debug("NotFoundException ignored for table {}", name);
        }

        if (canDeleteMetadata(name)) {
            // Delete the metadata.  Type doesn't matter since we discard the result
            log.info("Deleting user metadata for table {}", name);
            userMetadataService.deleteMetadata(metacatRequestContext.getUserName(), Lists.newArrayList(tableDto));
            log.info("Deleting tags for table {}", name);
            tagService.delete(name, false);
        } else {
            if (config.canSoftDeleteDataMetadata() && tableDto.isDataExternal()) {
                userMetadataService.softDeleteDataMetadata(metacatRequestContext.getUserName(),
                    Lists.newArrayList(tableDto.getDataUri()));
            }
        }
        eventBus.post(new MetacatDeleteTablePostEvent(name, metacatRequestContext, this, tableDto, isMView));
        return tableDto;
    }

    /**
     * Returns true
     * 1. If the system is configured to delete deifnition metadata.
     * 2. If the system is configured not to but the tableName is configured to either explicitly or if the
     * table's database/catalog is configure to.
     *
     * @param tableName table name
     * @return whether or not to delete definition metadata
     */
    private boolean canDeleteMetadata(final QualifiedName tableName) {
        return config.canDeleteTableDefinitionMetadata() || isEnabledForTableDefinitionMetadataDelete(tableName);
    }

    /**
     * Returns true if tableName is enabled for deifnition metadata delete either explicitly or if the
     * table's database/catalog is configure to.
     *
     * @param tableName table name
     * @return whether or not to delete definition metadata
     */
    private boolean isEnabledForTableDefinitionMetadataDelete(final QualifiedName tableName) {
        final Set<QualifiedName> enableDeleteForQualifiedNames = config.getNamesEnabledForDefinitionMetadataDelete();
        return enableDeleteForQualifiedNames.contains(tableName)
            || enableDeleteForQualifiedNames.contains(
            QualifiedName.ofDatabase(tableName.getCatalogName(), tableName.getDatabaseName()))
            || enableDeleteForQualifiedNames.contains(QualifiedName.ofCatalog(tableName.getCatalogName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<TableDto> get(final QualifiedName name, final GetTableServiceParameters getTableServiceParameters) {
        validate(name);
        TableDto tableInternal = null;
        final TableDto table;
        if (getTableServiceParameters.isIncludeInfo()
            || (getTableServiceParameters.isIncludeDefinitionMetadata()
            && !getTableServiceParameters.isDisableOnReadMetadataIntercetor())) {
            try {
                tableInternal = converterUtil.toTableDto(connectorTableServiceProxy
                    .get(name,
                        getTableServiceParameters,
                        getTableServiceParameters.isUseCache() && config.isCacheEnabled()));
            } catch (NotFoundException ignored) {
                return Optional.empty();
            }
            table = tableInternal;
        } else {
            table = new TableDto();
            table.setName(name);
        }

        if (getTableServiceParameters.isIncludeDefinitionMetadata()) {
            final Optional<ObjectNode> definitionMetadata =
                (getTableServiceParameters.isDisableOnReadMetadataIntercetor())
                    ? userMetadataService.getDefinitionMetadata(name)
                    : userMetadataService.getDefinitionMetadataWithInterceptor(name,
                    GetMetadataInterceptorParameters.builder().hasMetadata(tableInternal).build());
            definitionMetadata.ifPresent(table::setDefinitionMetadata);
        }

        if (getTableServiceParameters.isIncludeDataMetadata()) {
            TableDto dto = table;
            if (tableInternal == null && !getTableServiceParameters.isIncludeInfo()) {
                try {
                    dto = converterUtil.toTableDto(connectorTableServiceProxy
                        .get(name,
                            getTableServiceParameters,
                            getTableServiceParameters.isUseCache() && config.isCacheEnabled()));
                } catch (NotFoundException ignored) {
                }
            }
            if (dto != null && dto.getSerde() != null) {
                final Optional<ObjectNode> dataMetadata =
                    userMetadataService.getDataMetadata(dto.getSerde().getUri());
                dataMetadata.ifPresent(table::setDataMetadata);
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
        this.authorizationService.checkPermission(metacatRequestContext.getUserName(),
            oldName, MetacatOperation.RENAME);

        final TableDto oldTable = get(oldName, GetTableServiceParameters.builder()
            .includeInfo(true)
            .disableOnReadMetadataIntercetor(false)
            .includeDefinitionMetadata(true)
            .includeDataMetadata(true)
            .build()).orElseThrow(() -> new TableNotFoundException(oldName));
        if (oldTable != null) {
            //Ignore if the operation is not supported, so that we can at least go ahead and save the user metadata
            eventBus.post(new MetacatRenameTablePreEvent(oldName, metacatRequestContext, this, newName));
            connectorTableServiceProxy.rename(oldName, newName, isMView);
            userMetadataService.renameDefinitionMetadataKey(oldName, newName);
            tagService.renameTableTags(oldName, newName.getTableName());

            final TableDto dto = get(newName, GetTableServiceParameters.builder()
                .includeInfo(true)
                .disableOnReadMetadataIntercetor(false)
                .includeDefinitionMetadata(true)
                .includeDataMetadata(true)
                .build()).orElseThrow(() -> new IllegalStateException("should exist"));

            eventBus.post(
                new MetacatRenameTablePostEvent(oldName, metacatRequestContext, this, oldTable, dto, isMView));
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
        final TableDto oldTable = get(name, GetTableServiceParameters.builder()
            .disableOnReadMetadataIntercetor(false)
            .includeInfo(true)
            .includeDataMetadata(true)
            .includeDefinitionMetadata(true)
            .build()).orElseThrow(() -> new TableNotFoundException(name));
        eventBus.post(new MetacatUpdateTablePreEvent(name, metacatRequestContext, this, oldTable, tableDto));
        //
        // Check if the table schema info is provided. If provided, we should continue calling the update on the table
        // schema. Uri may exist in the serde when updating data metadata for a table.
        //
        boolean ignoreErrorsAfterUpdate = false;
        if (isTableInfoProvided(tableDto, oldTable)) {
            ignoreErrorsAfterUpdate = connectorTableServiceProxy.update(name, converterUtil.fromTableDto(tableDto));
        }

        try {
            // Merge in metadata if the user sent any
            if (tableDto.getDataMetadata() != null || tableDto.getDefinitionMetadata() != null) {
                log.info("Saving user metadata for table {}", name);
                final long start = registry.clock().wallTime();
                userMetadataService.saveMetadata(metacatRequestContext.getUserName(), tableDto, true);
                final long duration = registry.clock().wallTime() - start;
                log.info("Time taken to save user metadata for table {} is {} ms", name, duration);
                registry.timer(registry.createId(Metrics.TimerSaveTableMetadata.getMetricName()).withTags(name.parts()))
                    .record(duration, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            handleException(name, ignoreErrorsAfterUpdate, "saveMetadata", e);
        }

        TableDto updatedDto = tableDto;
        try {
            updatedDto = get(name,
                GetTableServiceParameters.builder()
                    .disableOnReadMetadataIntercetor(false)
                    .includeInfo(true)
                    .includeDataMetadata(true)
                    .includeDefinitionMetadata(true)
                    .build()).orElse(tableDto);
        } catch (Exception e) {
            handleException(name, ignoreErrorsAfterUpdate, "getTable", e);
        }

        try {
            eventBus.post(new MetacatUpdateTablePostEvent(name, metacatRequestContext, this, oldTable,
                updatedDto, updatedDto != tableDto));
        } catch (Exception e) {
            handleException(name, ignoreErrorsAfterUpdate, "postEvent", e);
        }
        return updatedDto;
    }

    /**
     * Throws exception if the provided <code>ignoreErrorsAfterUpdate</code> is false. If true, it will swallow the
     * exception and log it.
     *
     */
    private void handleException(final QualifiedName name,
                                 final boolean ignoreErrorsAfterUpdate,
                                 final String request,
                                 final Exception ex) {
        if (ignoreErrorsAfterUpdate) {
            log.warn("Failed {} for table {}", request, name);
            registry.counter(registry.createId(
                Metrics.CounterTableUpdateIgnoredException.getMetricName()).withTags(name.parts())
                .withTag("request", request)).increment();
        } else {
            throw Throwables.propagate(ex);
        }
    }


    @VisibleForTesting
    private boolean isTableInfoProvided(final TableDto tableDto, final TableDto oldTableDto) {
        boolean result = false;
        if ((tableDto.getFields() != null && !tableDto.getFields().isEmpty())
            || isSerdeInfoProvided(tableDto, oldTableDto)
            || (tableDto.getMetadata() != null && !tableDto.getMetadata().isEmpty())
            || tableDto.getAudit() != null) {
            result = true;
        }
        return result;
    }

    private boolean isSerdeInfoProvided(final TableDto tableDto, final TableDto oldTableDto) {
        boolean result = false;
        final StorageDto serde = tableDto.getSerde();
        if (serde == null) {
            result = false;
        } else {
            final StorageDto oldSerde = oldTableDto.getSerde();
            final String oldUri = oldSerde != null ? oldSerde.getUri() : null;
            if (serde.getInputFormat() != null
                || serde.getOutputFormat() != null
                || serde.getOwner() != null
                || serde.getParameters() != null
                || serde.getSerdeInfoParameters() != null
                || serde.getSerializationLib() != null
                || (serde.getUri() != null && !Objects.equals(serde.getUri(), oldUri))) {
                result = true;
            }
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
        //this is used for different purpose, need to change the ineral calls
        final Optional<TableDto> dto = get(name, GetTableServiceParameters.builder()
            .includeInfo(true)
            .includeDefinitionMetadata(true)
            .includeDataMetadata(true)
            .disableOnReadMetadataIntercetor(false)
            .build());
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
        final Optional<TableDto> oTable = get(sourceName,
            GetTableServiceParameters.builder()
                .includeInfo(true)
                .disableOnReadMetadataIntercetor(true)
                .includeDataMetadata(false)
                .includeDefinitionMetadata(false)
                .build());
        if (!oTable.isPresent()) {
            throw new TableNotFoundException(sourceName);
        }
        // Error out when target table already exists
        final Optional<TableDto> oTargetTable = get(targetName,
            GetTableServiceParameters.builder()
                .disableOnReadMetadataIntercetor(true)
                .includeInfo(true)
                .includeDataMetadata(false)
                .includeDefinitionMetadata(false)
                .build());
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
        final Optional<TableDto> tableDtoOptional = get(name, GetTableServiceParameters.builder().includeInfo(true)
            .disableOnReadMetadataIntercetor(true)
            .includeDefinitionMetadata(false)
            .includeDataMetadata(false)
            .build());
        if (tableDtoOptional.isPresent()) {
            final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
            final TableDto tableDto = tableDtoOptional.get();
            tableDto.setDefinitionMetadata(definitionMetadata); //override the previous one
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
        return connectorTableServiceProxy.getQualifiedNames(uri, prefixSearch);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<QualifiedName>> getQualifiedNames(final List<String> uris, final boolean prefixSearch) {
        return connectorTableServiceProxy.getQualifiedNames(uris, prefixSearch);
    }

    @Override
    public List<QualifiedName> getQualifiedNames(final QualifiedName name,
                                                 final GetTableNamesServiceParameters parameters) {
        if (Strings.isNullOrEmpty(parameters.getFilter())) {
            throw new MetacatBadRequestException("Filter expression cannot be empty");
        }
        return connectorTableServiceProxy.getQualifiedNames(name, parameters);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean exists(final QualifiedName name) {
        return connectorTableServiceProxy.exists(name);
    }

    private void validate(final QualifiedName name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkArgument(name.isTableDefinition(), "Definition {} does not refer to a table", name);
    }
}
