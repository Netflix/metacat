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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.connectors.exception.NotFoundException;
import com.netflix.metacat.common.server.connectors.exception.TableMigrationInProgressException;
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException;

import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatCreateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatCreateTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatDeleteTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.events.MetacatRenameTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatRenameTablePreEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateIcebergTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePostEvent;
import com.netflix.metacat.common.server.events.MetacatUpdateTablePreEvent;
import com.netflix.metacat.common.server.model.ChildInfo;
import com.netflix.metacat.common.server.model.ParentInfo;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;
import com.netflix.metacat.common.server.usermetadata.AuthorizationService;
import com.netflix.metacat.common.server.usermetadata.GetMetadataInterceptorParameters;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataConstants;
import com.netflix.metacat.common.server.usermetadata.MetacatOperation;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.usermetadata.ParentChildRelMetadataService;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.common.server.util.MetacatUtils;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.GetTableNamesServiceParameters;
import com.netflix.metacat.main.services.GetTableServiceParameters;

import com.netflix.metacat.main.services.OwnerValidationService;
import com.netflix.metacat.main.services.TableService;
import com.netflix.spectator.api.Registry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import javax.annotation.Nullable;


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
@RequiredArgsConstructor
public class TableServiceImpl implements TableService {
    private final ConnectorManager connectorManager;
    private final ConnectorTableServiceProxy connectorTableServiceProxy;
    private final DatabaseService databaseService;
    private final TagService tagService;
    private final UserMetadataService userMetadataService;
    private final MetacatJson metacatJson;
    private final MetacatEventBus eventBus;
    private final Registry registry;
    private final Config config;
    private final ConverterUtil converterUtil;
    private final AuthorizationService authorizationService;
    private final OwnerValidationService ownerValidationService;
    private final ParentChildRelMetadataService parentChildRelMetadataService;

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto create(final QualifiedName name, final TableDto tableDto) {
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        validate(name);
        this.authorizationService.checkPermission(metacatRequestContext.getUserName(),
            tableDto.getName(), MetacatOperation.CREATE);

        setDefaultAttributes(tableDto);
        ownerValidationService.enforceOwnerValidation("createTable", name, tableDto);

        log.info("Creating table {}", name);
        eventBus.post(new MetacatCreateTablePreEvent(name, metacatRequestContext, this, tableDto));

        final Optional<Runnable> unSaveOpOpt = saveParentChildRelationship(name, tableDto);
        try {
            connectorTableServiceProxy.create(name, converterUtil.fromTableDto(tableDto));
        } catch (Exception e) {
            unSaveOpOpt.ifPresent(Runnable::run);
            throw e;
        }

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

        TableDto dto = tableDto;
        try {
            dto = get(name, GetTableServiceParameters.builder()
                .disableOnReadMetadataIntercetor(false)
                .includeInfo(true)
                .includeDataMetadata(true)
                .includeDefinitionMetadata(true)
                .build()).orElse(tableDto);
        } catch (Exception e) {
            handleExceptionOnCreate(name, "getTable", e);
        }

        try {
            eventBus.post(new MetacatCreateTablePostEvent(name, metacatRequestContext, this, dto));
        } catch (Exception e) {
            handleExceptionOnCreate(name, "postEvent", e);
        }
        return dto;
    }

    private ObjectNode createParentChildObjectNode(final QualifiedName name) {
        final Set<ParentInfo> parentInfos = parentChildRelMetadataService.getParents(name);

        final ObjectMapper objectMapper = new ObjectMapper();
        final ObjectNode rootNode = objectMapper.createObjectNode();

        // For childTable, we will always populate the parent infos
        if (parentInfos != null && !parentInfos.isEmpty()) {
            final ArrayNode parentArrayNode = objectMapper.createArrayNode();
            for (ParentInfo parentInfo : parentInfos) {
                final ObjectNode parentNode = objectMapper.createObjectNode();
                parentNode.put("name", parentInfo.getName());
                parentNode.put("relationType", parentInfo.getRelationType());
                parentNode.put("uuid", parentInfo.getUuid());
                parentArrayNode.add(parentNode);
            }
            rootNode.set(ParentChildRelMetadataConstants.PARENT_INFOS, parentArrayNode);
        }

        // For parent table, if it has a child, we will put a field to indicate that the table is a parent table.
        if (parentChildRelMetadataService.isParentTable(name)) {
            rootNode.put(ParentChildRelMetadataConstants.IS_PARENT, true);
        }
        return rootNode;
    }

    // Return the unSaveOperation if applicable
    private Optional<Runnable> saveParentChildRelationship(final QualifiedName child, final TableDto tableDto) {
        if (tableDto.getDefinitionMetadata() != null) {
            final ObjectNode definitionMetadata = tableDto.getDefinitionMetadata();
            if (definitionMetadata.has(ParentChildRelMetadataConstants.PARENT_CHILD_RELINFO)) {

                if (!config.isParentChildCreateEnabled()) {
                    throw new RuntimeException("parent child creation is currently disabled");
                }

                final JsonNode parentChildRelInfo =
                    definitionMetadata.get(ParentChildRelMetadataConstants.PARENT_CHILD_RELINFO);

                String parentName;
                if (!parentChildRelInfo.has(ParentChildRelMetadataConstants.PARENT_NAME)) {
                    throw new IllegalArgumentException("parent name is not specified");
                }
                parentName = parentChildRelInfo.path(ParentChildRelMetadataConstants.PARENT_NAME)
                    .asText();
                final QualifiedName parent = QualifiedName.fromString(parentName);
                validate(parent);
                if (!exists(parent)) {
                    throw new IllegalArgumentException("Parent Table:" + parent + " does not exist");
                }

                // fetch parent and child uuid
                String parentUUID;
                String childUUID;
                if (!parentChildRelInfo.has(ParentChildRelMetadataConstants.PARENT_UUID)) {
                    throw new IllegalArgumentException("parent_table_uuid is not specified for parent table="
                            + parentName);
                }

                if (!parentChildRelInfo.has(ParentChildRelMetadataConstants.CHILD_UUID)) {
                    throw new IllegalArgumentException("child_table_uuid is not specified for child table=" + child);
                }
                parentUUID = parentChildRelInfo.path(ParentChildRelMetadataConstants.PARENT_UUID).asText();
                childUUID = parentChildRelInfo.path(ParentChildRelMetadataConstants.CHILD_UUID).asText();

                // fetch relationshipType
                String relationType;
                if (parentChildRelInfo.has(ParentChildRelMetadataConstants.RELATION_TYPE)) {
                    relationType = parentChildRelInfo.path(ParentChildRelMetadataConstants.RELATION_TYPE).asText();
                } else {
                    relationType = "CLONE";
                }

                // Create parent child relationship
                parentChildRelMetadataService.createParentChildRelation(parent, parentUUID,
                    child, childUUID, relationType, config.getParentChildRelationshipProperties());

                // Return a Runnable for deleting the relationship
                return Optional.of(() -> {
                    try {
                        parentChildRelMetadataService.deleteParentChildRelation(parent,
                            parentUUID, child, childUUID, relationType);
                    } catch (Exception e) {
                        log.error("parentChildRelMetadataService: Failed to delete parent child relationship "
                                + "after failing to create the table={} with the following parameters: "
                                + "parent={}, parentUUID={}, child={}, childUUID={}, relationType={}",
                            child, parent, parentUUID, child, childUUID, relationType, e);
                    }
                });
            }
        }
        return Optional.empty();
    }

    private void setDefaultAttributes(final TableDto tableDto) {
        setDefaultSerdeIfNull(tableDto);
        setDefaultDefinitionMetadataIfNull(tableDto);
        setOwnerIfNull(tableDto);
        setOwnerGroupIfAvailable(tableDto);
    }

    private void setDefaultDefinitionMetadataIfNull(final TableDto tableDto) {
        ObjectNode definitionMetadata = tableDto.getDefinitionMetadata();
        if (definitionMetadata == null) {
            definitionMetadata = metacatJson.emptyObjectNode();
            tableDto.setDefinitionMetadata(definitionMetadata);
        }
    }

    private void setDefaultSerdeIfNull(final TableDto tableDto) {
        StorageDto serde = tableDto.getSerde();
        if (serde == null) {
            serde = new StorageDto();
            tableDto.setSerde(serde);
        }
    }

    /**
     * Sets the owner of the table. The order of priority of selecting the owner is:
     * <pre>
     *     1. Explicitly set in the table dto
     *     2. Username from the request headers
     *     3. Owner set in the serde
     * </pre>
     *
     * @param tableDto the table DTO
     */
    private void setOwnerIfNull(final TableDto tableDto) {
        final List<String> potentialOwners = ownerValidationService.extractPotentialOwners(tableDto);

        final String validOwner = potentialOwners.stream()
                                      .filter(this::isOwnerValid)
                                      .findFirst()
                                      .orElse(null);

        if (validOwner != null) {
            updateTableOwner(tableDto, validOwner);
        } else {
            potentialOwners.stream()
                .filter(Objects::nonNull)
                .findFirst()
                .ifPresent(nonNullOwner -> updateTableOwner(tableDto, nonNullOwner));
        }
    }

    private void setOwnerGroupIfAvailable(final TableDto tableDto) {
        final List<String> potentialOwnerGroups = ownerValidationService.extractPotentialOwnerGroups(tableDto);

        potentialOwnerGroups.stream()
            .filter(this::isOwnerGroupValid)
            .findFirst()
            .ifPresent(validOwnerGroup -> updateTableOwnerGroup(tableDto, validOwnerGroup));

    }

    void updateTableOwner(final TableDto tableDto, final String userId) {
        final ObjectNode ownerNode = tableDto.getDefinitionMetadata().with("owner");
        ownerNode.put("userId", userId);
    }

    void updateTableOwnerGroup(final TableDto tableDto, final String groupName) {
        final ObjectNode ownerNode = tableDto.getDefinitionMetadata().with("owner");
        ownerNode.put("google_group", groupName);
    }

    private boolean isOwnerValid(@Nullable final String userId) {
        return ownerValidationService.isUserValid(userId);
    }

    private boolean isOwnerGroupValid(@Nullable final String groupName) {
        return ownerValidationService.isGroupValid(groupName);
    }

    @SuppressFBWarnings
    private void tag(final QualifiedName name, final ObjectNode definitionMetadata) {
        final Set<String> tags = MetacatUtils.getTableTags(definitionMetadata);
        if (!tags.isEmpty()) {
            log.info("Setting tags {} for table {}", tags, name);
            final Set<String> result = tagService.setTags(name, tags, false);
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

        // Fail if the table is tagged not to be deleted.
        if (hasTags(tableDto, config.getNoTableDeleteOnTags())) {
            if (MetacatUtils.hasDoNotModifyForIcebergMigrationTag(tableDto, config.getNoTableDeleteOnTags())) {
                throw new TableMigrationInProgressException(
                        MetacatUtils.getIcebergMigrationExceptionMsg("Delete", name.toString()));
            } else {
                throw new IllegalArgumentException(
                        String.format("Table %s cannot be deleted because it is tagged with %s.", name,
                                config.getNoTableDeleteOnTags()));
            }
        }

        if (!config.isParentChildDropEnabled()) {
            if (parentChildRelMetadataService.isChildTable(name)) {
                throw new RuntimeException(name + " is a child table and "
                    + "dropping a child table is currently disabled");
            }
            if (parentChildRelMetadataService.isParentTable(name)) {
                throw new RuntimeException(name + " is a parent table and "
                    + "dropping a parent table is currently disabled");
            }
        }

        final Set<ChildInfo> childInfos = parentChildRelMetadataService.getChildren(name);
        if (childInfos != null && !childInfos.isEmpty()) {
            final StringBuilder errorSb = new StringBuilder();
            errorSb.append("Failed to drop ").append(name)
                .append(" because it still has ")
                .append(childInfos.size())
                .append(" child table(s). One example is:");

            errorSb.append("\n").append(childInfos.iterator().next().getName());

            throw new RuntimeException(errorSb.toString());
        }
        try {
            connectorTableServiceProxy.delete(name);
            // If this is a common view, the storage_table if present
            // should also be deleted.
            if (MetacatUtils.isCommonView(tableDto.getMetadata())
                    && config.deleteCommonViewStorageTable()) {
                final Optional<String> storageTableName = MetacatUtils
                        .getCommonViewStorageTable(tableDto.getMetadata());
                if (storageTableName.isPresent()) {
                    final QualifiedName qualifiedStorageTableName = QualifiedName.ofTable(name.getCatalogName(),
                            name.getDatabaseName(), storageTableName.get());
                    deleteCommonViewStorageTable(name, qualifiedStorageTableName);
                }
            }
        } catch (NotFoundException ignored) {
            log.debug("NotFoundException ignored for table {}", name);
        }

        try {
            parentChildRelMetadataService.drop(name);
        } catch (Exception e) {
            log.error("parentChildRelMetadataService: Failed to drop relation for table={}", name, e);
        }

        if (canDeleteMetadata(name)) {
            // Delete the metadata.  Type doesn't matter since we discard the result
            log.info("Deleting user metadata for table {}", name);
            userMetadataService.deleteMetadata(metacatRequestContext.getUserName(), Lists.newArrayList(tableDto));
            log.info("Deleting tags for table {}", name);
            tagService.delete(name, false);
        } else {
            if (config.canSoftDeleteDataMetadata() && tableDto.isDataExternal()) {
                // Remove the migrated_data_location field before soft deleting to allow for cleanup of old prefixes
                if (tableDto.getDefinitionMetadata() != null
                    && tableDto.getDefinitionMetadata().has(MetacatUtils.MIGRATED_DATA_LOCATION)) {
                    tableDto.getDefinitionMetadata().remove(MetacatUtils.MIGRATED_DATA_LOCATION);
                    userMetadataService.saveDefinitionMetadata(tableDto.getName(),
                        metacatRequestContext.getUserName(),
                        Optional.of(tableDto.getDefinitionMetadata()),
                            false);
                }
                userMetadataService.softDeleteDataMetadata(metacatRequestContext.getUserName(),
                    Lists.newArrayList(tableDto.getDataUri()));
            }
        }
        eventBus.post(new MetacatDeleteTablePostEvent(name, metacatRequestContext, this, tableDto, isMView));
        return tableDto;
    }

    private boolean hasTags(@Nullable final TableDto tableDto, final Set<String> hasTags) {
        if (!hasTags.isEmpty() && tableDto != null) {
            final Set<String> tags = MetacatUtils.getTableTags(tableDto.getDefinitionMetadata());
            if (!tags.isEmpty()) {
                for (String t: hasTags) {
                    if (tags.contains(t)) {
                        return true;
                    }
                }
            }
        }
        return false;
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
        final MetacatCatalogConfig catalogConfig = connectorManager.getCatalogConfig(name);
        if (getTableServiceParameters.isIncludeInfo()
            || (getTableServiceParameters.isIncludeDefinitionMetadata() && catalogConfig.isInterceptorEnabled()
            && !getTableServiceParameters.isDisableOnReadMetadataIntercetor())) {
            try {
                final boolean useCache = getTableServiceParameters.isUseCache() && config.isCacheEnabled()
                    && catalogConfig.isCacheEnabled();
                tableInternal = converterUtil.toTableDto(
                    getFromTableServiceProxy(name, getTableServiceParameters, useCache));
            } catch (NotFoundException ignored) {
                return Optional.empty();
            }
            table = tableInternal;
        } else {
            table = new TableDto();
            table.setName(name);
        }

        if (getTableServiceParameters.isIncludeDefinitionMetadata()) {
            Optional<ObjectNode> definitionMetadata =
                (getTableServiceParameters.isDisableOnReadMetadataIntercetor())
                    ? userMetadataService.getDefinitionMetadata(name)
                    : userMetadataService.getDefinitionMetadataWithInterceptor(name,
                    GetMetadataInterceptorParameters.builder().hasMetadata(tableInternal).build());
            // Always get the source of truth for parent child relation from the parentChildRelMetadataService
            if (definitionMetadata.isPresent()
                && definitionMetadata.get().has(ParentChildRelMetadataConstants.PARENT_CHILD_RELINFO)) {
                definitionMetadata.get().remove(ParentChildRelMetadataConstants.PARENT_CHILD_RELINFO);
            }
            if (config.isParentChildGetEnabled()) {
                final ObjectNode parentChildRelObjectNode = createParentChildObjectNode(name);
                if (!parentChildRelObjectNode.isEmpty()) {
                    if (!definitionMetadata.isPresent()) {
                        definitionMetadata = Optional.of(new ObjectMapper().createObjectNode());
                    }
                    definitionMetadata.get().set(ParentChildRelMetadataConstants.PARENT_CHILD_RELINFO,
                        parentChildRelObjectNode);
                }
            }
            definitionMetadata.ifPresent(table::setDefinitionMetadata);
        }

        if (getTableServiceParameters.isIncludeDataMetadata() && catalogConfig.isHasDataExternal()) {
            TableDto dto = table;
            if (tableInternal == null && !getTableServiceParameters.isIncludeInfo()) {
                try {
                    final boolean useCache = getTableServiceParameters.isUseCache() && config.isCacheEnabled();
                    dto = converterUtil.toTableDto(
                        getFromTableServiceProxy(name, getTableServiceParameters, useCache));
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

        // Fail if the table is tagged not to be renamed.
        if (hasTags(oldTable, config.getNoTableRenameOnTags())) {
            if (MetacatUtils.hasDoNotModifyForIcebergMigrationTag(oldTable, config.getNoTableRenameOnTags())) {
                throw new TableMigrationInProgressException(
                        MetacatUtils.getIcebergMigrationExceptionMsg("Rename", oldName.toString()));
            } else {
                throw new IllegalArgumentException(
                        String.format("Table %s cannot be renamed because it is tagged with %s.", oldName,
                                config.getNoTableRenameOnTags()));
            }
        }

        if (oldTable != null) {
            //Ignore if the operation is not supported, so that we can at least go ahead and save the user metadata
            eventBus.post(new MetacatRenameTablePreEvent(oldName, metacatRequestContext, this, newName));

            if (!config.isParentChildRenameEnabled()) {
                if (parentChildRelMetadataService.isChildTable(oldName)) {
                    throw new RuntimeException(oldName + " is a child table and "
                        + "renaming on child table is currently disabled");
                }
                if (parentChildRelMetadataService.isParentTable(oldName)) {
                    throw new RuntimeException(oldName + " is a parent table and "
                        + "renaming on parent table is currently disabled");
                }
            }

            // Before rename, first rename its parent child relation
            parentChildRelMetadataService.rename(oldName, newName);

            try {
                connectorTableServiceProxy.rename(oldName, newName, isMView);
            } catch (Exception e) {
                try {
                    // if rename operation fail, rename back the parent child relation
                    parentChildRelMetadataService.rename(newName, oldName);
                } catch (Exception renameException) {
                    log.error("parentChildRelMetadataService: Failed to rename parent child relation "
                            + "after table fail to rename from {} to {} "
                            + "with the following parameters oldName={} to newName={}",
                        oldName, newName, oldName, newName, renameException);
                }
                throw e;
            }
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
        return updateAndReturn(name, tableDto, false);
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
            log.warn("Failed {} for table {}. Error: {}", request, name, ex.getMessage());
            registry.counter(registry.createId(
                Metrics.CounterTableUpdateIgnoredException.getMetricName()).withTags(name.parts())
                .withTag("request", request)).increment();
        } else {
            throw Throwables.propagate(ex);
        }
    }

    /**
     * Swallow the exception and log it.
     *
     */
    private void handleExceptionOnCreate(final QualifiedName name,
                                         final String request,
                                         final Exception ex) {
        log.warn("Failed {} for create table {}. Error: {}", request, name, ex.getMessage());
        registry.counter(registry.createId(
            Metrics.CounterTableCreateIgnoredException.getMetricName()).withTags(name.parts())
            .withTag("request", request)).increment();
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

    @Override
    public TableDto updateAndReturn(final QualifiedName name, final TableDto tableDto,
                                    final boolean shouldThrowExceptionOnMetadataSaveFailure) {
        validate(name);
        final MetacatRequestContext metacatRequestContext = MetacatContextManager.getContext();
        final TableDto oldTable = get(name, GetTableServiceParameters.builder()
            .disableOnReadMetadataIntercetor(false)
            .includeInfo(true)
            .includeDataMetadata(true)
            .includeDefinitionMetadata(true)
            .build()).orElseThrow(() -> new TableNotFoundException(name));
        eventBus.post(new MetacatUpdateTablePreEvent(name, metacatRequestContext, this, oldTable, tableDto));

        if (MetacatUtils.hasDoNotModifyForIcebergMigrationTag(oldTable, config.getNoTableUpdateOnTags())) {
            throw new TableMigrationInProgressException(
                MetacatUtils.getIcebergMigrationExceptionMsg("Updates", name.toString()));
        }



        //
        // Check if the table schema info is provided. If provided, we should continue calling the update on the table
        // schema. Uri may exist in the serde when updating data metadata for a table.
        //
        boolean ignoreErrorsAfterUpdate = false;
        if (isTableInfoProvided(tableDto, oldTable)) {
            ignoreErrorsAfterUpdate = connectorTableServiceProxy.update(name, converterUtil.fromTableDto(tableDto));
        }

        // we do ownership validation and enforcement only if table owner is set in the dto
        // because if it is null, we do not update the owner in the existing metadata record
        if (tableDto.getTableOwner().isPresent()) {
            // only if the owner is different from the previous, we run the enforcement
            // for backwards compatibility
            if (!tableDto.getTableOwner().get().equals(oldTable.getTableOwner().orElse(null))) {
                ownerValidationService.enforceOwnerValidation("updateTable", name, tableDto);
            }
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
            if (shouldThrowExceptionOnMetadataSaveFailure) {
                handleException(name, false, "saveMetadata", e);
            } else {
                handleException(name, ignoreErrorsAfterUpdate, "saveMetadata", e);
            }
        }

        // ignoreErrorsAfterUpdate is currently set only for iceberg tables
        if (config.isUpdateIcebergTableAsyncPostEventEnabled() && ignoreErrorsAfterUpdate) {
            eventBus.post(new MetacatUpdateIcebergTablePostEvent(name,
                metacatRequestContext, this, oldTable, tableDto));
            return tableDto;
        } else {
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
    }

    private TableInfo getFromTableServiceProxy(final QualifiedName name,
                                               final GetTableServiceParameters getTableServiceParameters,
                                               final boolean useCache) {
        return getTableServiceParameters.isIncludeMetadataLocationOnly()
                ? connectorTableServiceProxy.getWithMetadataLocationOnly(name, getTableServiceParameters, useCache)
                : (getTableServiceParameters.isIncludeMetadataFromConnector()
                        ? connectorTableServiceProxy.getWithInfoDetails(name, getTableServiceParameters, useCache)
                        : connectorTableServiceProxy.get(name, getTableServiceParameters, useCache));
    }

    private void deleteCommonViewStorageTable(final QualifiedName viewName,
                                              final QualifiedName storageTableName) {

        try {
            log.warn("Deleting storage table: {} belonging to common view: {}",
                    storageTableName, viewName);
            deleteAndReturn(storageTableName, false);
        } catch (Exception e) {
            // For now only register failures to drop
            handleException(storageTableName, true, "deleteCommonViewStorageTable", e);
        }
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
