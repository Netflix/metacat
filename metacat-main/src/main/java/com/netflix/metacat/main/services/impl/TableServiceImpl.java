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
import com.facebook.presto.spi.StorageInfo;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.usermetadata.TagService;
import com.netflix.metacat.common.usermetadata.UserMetadataService;
import com.netflix.metacat.converters.PrestoConverters;
import com.netflix.metacat.main.connector.MetacatConnectorManager;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.SessionProvider;
import com.netflix.metacat.main.services.TableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableServiceImpl implements TableService {
    private static final Logger log = LoggerFactory.getLogger(TableServiceImpl.class);
    @Inject
    MetacatConnectorManager metacatConnectorManager;
    @Inject
    MetadataManager metadataManager;
    @Inject
    PrestoConverters prestoConverters;
    @Inject
    SessionProvider sessionProvider;
    @Inject
    UserMetadataService userMetadataService;
    @Inject
    DatabaseService databaseService;
    @Inject
    TagService tagService;
    @Inject
    MViewService mViewService;
    private static final String NAME_TAGS = "tags";

    @Override
    public void create(@Nonnull QualifiedName name, @Nonnull TableDto tableDto) {
        Session session = validateAndGetSession(name);
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
            tag( name, tableDto.getDefinitionMetadata());
        }
    }

    private void setOwnerIfNull(TableDto tableDto, String user) {
        if(!Strings.isNullOrEmpty(user)) {
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

    private void tag(QualifiedName name, ObjectNode definitionMetadata) {
        if( definitionMetadata != null && definitionMetadata.get(NAME_TAGS) != null){
            JsonNode tagsNode = definitionMetadata.get(NAME_TAGS);
            Set<String> tags = Sets.newHashSet();
            if( tagsNode.isArray() && tagsNode.size() > 0){
                for(JsonNode tagNode: tagsNode){
                    tags.add( tagNode.textValue());
                }
                log.info("Setting tags {} for table {}", tags, name);
                tagService.setTableTags( name, tags, false);
            }
        }
    }

    @Override
    public TableDto deleteAndReturn(@Nonnull QualifiedName name) {
        Session session = validateAndGetSession(name);
        QualifiedTableName tableName = prestoConverters.getQualifiedTableName(name);

        Optional<TableHandle> tableHandle = metadataManager.getTableHandle(session, tableName);
        Optional<TableDto> oTable = get(name, true);
        if (oTable.isPresent()) {
            log.info("Drop table {}", name);
            metadataManager.dropTable(session, tableHandle.get());
        }

        TableDto tableDto = oTable.orElseGet(() -> {
            // If the table doesn't exist construct a blank copy we can use to delete the definitionMetadata and tags
            TableDto t = new TableDto();
            t.setName(name);
            return t;
        });

        // Delete the metadata.  Type doesn't matter since we discard the result
        log.info("Deleting user metadata for table {}", name);
        userMetadataService.deleteMetadatas(Lists.newArrayList(tableDto), false);
        log.info("Deleting tags for table {}", name);
        tagService.delete(name, false);

        return tableDto;
    }

    @Override
    public Optional<TableDto> get(@Nonnull QualifiedName name, boolean includeUserMetadata) {
        return get( name, true, includeUserMetadata, includeUserMetadata);
    }

    @Override
    public Optional<TableDto> get(@Nonnull QualifiedName name, boolean includeInfo, boolean includeDefinitionMetadata, boolean includeDataMetadata) {
        Session session = validateAndGetSession(name);
        TableDto table;
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
            String type = metacatConnectorManager.getCatalogConfig(name).getType();
            table = prestoConverters.toTableDto(name, type, tableMetadata.get());
        } else {
            table = new TableDto();
            table.setName(name);
        }

        if (includeDefinitionMetadata) {
            Optional<ObjectNode> definitionMetadata = userMetadataService.getDefinitionMetadata(name);
            if (definitionMetadata.isPresent()) {
                table.setDefinitionMetadata(definitionMetadata.get());
            }
        }

        if (includeDataMetadata) {
            if (!tableMetadata.isPresent()) {
                tableMetadata = Optional.ofNullable(getTableMetadata(name, session));
            }
            if (tableMetadata.isPresent()) {
                ConnectorTableMetadata connectorTableMetadata = tableMetadata.get().getMetadata();
                if (connectorTableMetadata instanceof ConnectorTableDetailMetadata) {
                    ConnectorTableDetailMetadata detailMetadata = (ConnectorTableDetailMetadata) connectorTableMetadata;
                    StorageInfo storageInfo = detailMetadata.getStorageInfo();
                    if (storageInfo != null) {
                        Optional<ObjectNode> dataMetadata = userMetadataService.getDataMetadata(storageInfo.getUri());
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
    public Optional<TableHandle> getTableHandle(@Nonnull QualifiedName name) {
        Session session = validateAndGetSession(name);

        QualifiedTableName qualifiedTableName = prestoConverters.getQualifiedTableName(name);
        return metadataManager.getTableHandle(session, qualifiedTableName);
    }

    private TableMetadata getTableMetadata(QualifiedName name, Optional<TableHandle> tableHandle) {
        if (!tableHandle.isPresent()) {
            return null;
        }
        Session session = validateAndGetSession(name);
        TableMetadata result = metadataManager.getTableMetadata(session, tableHandle.get());
        checkState(name.getDatabaseName().equals(result.getTable().getSchemaName()), "Unexpected database");
        checkState(name.getTableName().equals(result.getTable().getTableName()), "Unexpected table");

        return result;
    }

    private TableMetadata getTableMetadata(QualifiedName name, Session session) {
        QualifiedTableName qualifiedTableName = prestoConverters.getQualifiedTableName(name);
        Optional<TableHandle> tableHandle = metadataManager.getTableHandle(session, qualifiedTableName);
        return getTableMetadata(name, tableHandle);
    }

    @Override
    public void rename(@Nonnull QualifiedName oldName, @Nonnull QualifiedName newName, boolean isMView) {
        Session session = validateAndGetSession(oldName);

        QualifiedTableName oldPrestoName = prestoConverters.getQualifiedTableName(oldName);
        QualifiedTableName newPrestoName = prestoConverters.getQualifiedTableName(newName);

        Optional<TableHandle> tableHandle = metadataManager.getTableHandle(session, oldPrestoName);
        if (tableHandle.isPresent()) {
            //Ignore if the operation is not supported, so that we can at least go ahead and save the user metadata
            try {
                log.info("Renaming {} {} to {}", isMView?"view":"table", oldName, newName);
                metadataManager.renameTable(session, tableHandle.get(), newPrestoName);

                if( !isMView) {
                    final String prefix = String.format("%s_%s_", oldName.getDatabaseName(),
                            MoreObjects.firstNonNull(oldName.getTableName(), ""));
                    List<NameDateDto> views = mViewService.list(oldName);
                    if (views != null && !views.isEmpty()) {
                        views.forEach(view -> {
                            QualifiedName newViewName = QualifiedName.ofView(oldName.getCatalogName(), oldName.getDatabaseName(), newName.getTableName(), view.getName().getViewName());
                            mViewService.rename(view.getName(), newViewName);
                        });
                    }
                }
            } catch(PrestoException e){
                if(!NOT_SUPPORTED.toErrorCode().equals(e.getErrorCode())){
                    throw e;
                }
            }
            userMetadataService.renameDefinitionMetadataKey(oldName, newName);
            tagService.rename(oldName, newName.getTableName());
        }
    }

    @Override
    public void update(@Nonnull QualifiedName name, @Nonnull TableDto tableDto) {
        Session session = validateAndGetSession(name);

        //Ignore if the operation is not supported, so that we can at least go ahead and save the user metadata
        try {
            TypeManager typeManager = metadataManager.getTypeManager();
            log.info("Updating table {}", name);
            metadataManager.alterTable(session, prestoConverters.fromTableDto(name, tableDto, typeManager));
        } catch(PrestoException e){
            if(!NOT_SUPPORTED.toErrorCode().equals(e.getErrorCode())){
                throw e;
            }
        }

        // Merge in metadata if the user sent any
        if (tableDto.getDataMetadata() != null || tableDto.getDefinitionMetadata() != null) {
            log.info("Saving user metadata for table {}", name);
            userMetadataService.saveMetadata(session.getUser(), tableDto, true);
        }
    }

    @Override
    public void delete(@Nonnull QualifiedName name) {
        deleteAndReturn(name);
    }

    @Override
    public TableDto get(@Nonnull QualifiedName name) {
        Optional<TableDto> dto = get( name, true);
        return dto.orElse(null);
    }

    @Override
    public TableDto copy( @Nonnull QualifiedName sourceName, @Nonnull QualifiedName targetName) {
        // Source should be same
        if( !sourceName.getCatalogName().equals(targetName.getCatalogName())){
            throw new MetacatNotSupportedException("Cannot copy a table from a different source");
        }
        // Error out when source table does not exists
        Optional<TableDto> oTable = get(sourceName, false);
        if( !oTable.isPresent()){
            throw new TableNotFoundException(new SchemaTableName(sourceName.getDatabaseName(), sourceName.getTableName()));
        }
        // Error out when target table already exists
        Optional<TableDto> oTargetTable = get( targetName, false);
        if( oTargetTable.isPresent()){
            throw new TableNotFoundException(new SchemaTableName(targetName.getDatabaseName(), targetName.getTableName()));
        }
        return copy(oTable.get(), targetName);
    }

    @Override
    public TableDto copy(@Nonnull TableDto tableDto, @Nonnull QualifiedName targetName) {
        if( !databaseService.exists( targetName)){
            databaseService.create( targetName, null);
        }
        TableDto targetTableDto = new TableDto();
        targetTableDto.setName( targetName);
        targetTableDto.setFields(tableDto.getFields());
        targetTableDto.setPartition_keys( tableDto.getPartition_keys());
        StorageDto storageDto = tableDto.getSerde();
        if( storageDto != null) {
            StorageDto targetStorageDto = new StorageDto();
            targetStorageDto.setInputFormat(storageDto.getInputFormat());
            targetStorageDto.setOwner(storageDto.getOwner());
            targetStorageDto.setOutputFormat(storageDto.getOutputFormat());
            targetStorageDto.setParameters(storageDto.getParameters());
            targetStorageDto.setUri(storageDto.getUri());
            targetStorageDto.setSerializationLib(storageDto.getSerializationLib());
            targetTableDto.setSerde(targetStorageDto);
        }
        create( targetName, targetTableDto);
        return targetTableDto;
    }

    @Override
    public void saveMetadata(
            @Nonnull
            QualifiedName name, ObjectNode definitionMetadata, ObjectNode dataMetadata) {
        Session session = validateAndGetSession(name);
        Optional<TableDto> tableDtoOptional = get( name, false);
        if( tableDtoOptional.isPresent()){
            TableDto tableDto = tableDtoOptional.get();
            tableDto.setDefinitionMetadata( definitionMetadata);
            tableDto.setDataMetadata( dataMetadata);
            log.info("Saving user metadata for table {}", name);
            userMetadataService.saveMetadata( session.getUser(), tableDto, true);
            tag( name, tableDto.getDefinitionMetadata());
        }
    }

    @Override
    public List<QualifiedName> getQualifiedNames(String uri, boolean prefixSearch){
        List<QualifiedName> result = Lists.newArrayList();
        Map<String, String> catalogNames = metadataManager.getCatalogNames();

        catalogNames.values().stream().forEach(catalogName -> {
            Session session = sessionProvider.getSession(QualifiedName.ofCatalog(catalogName));
            List<SchemaTableName> schemaTableNames = metadataManager.getTableNames( session, uri, prefixSearch);
            List<QualifiedName> qualifiedNames = schemaTableNames.stream().map(
                    schemaTableName -> QualifiedName.ofTable( catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName())).collect(Collectors.toList());
            result.addAll(qualifiedNames);
        });
        return result;
    }

    @Override
    public boolean exists(@Nonnull QualifiedName name) {
        return get(name, true, false, false).isPresent();
    }

    private Session validateAndGetSession(QualifiedName name) {
        checkNotNull(name, "name cannot be null");
        checkArgument(name.isTableDefinition(), "Definition {} does not refer to a table", name);

        return sessionProvider.getSession(name);
    }
}
