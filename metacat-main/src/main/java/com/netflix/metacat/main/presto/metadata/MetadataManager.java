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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.metacat.main.presto.metadata;

import com.facebook.presto.Session;
import com.facebook.presto.exception.CatalogNotFoundException;
import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ConnectorDetailMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSchemaMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import javax.inject.Inject;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.QualifiedTableName.convertFromSchemaTableName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

public class MetadataManager
{
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectorsByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectorsById = new ConcurrentHashMap<>();
    private final TypeManager typeManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final TablePropertyManager tablePropertyManager;

    @Inject
    public MetadataManager(
            TypeManager typeManager,
            SessionPropertyManager sessionPropertyManager,
            TablePropertyManager tablePropertyManager)
    {
        this.typeManager = checkNotNull(typeManager, "types is null");
        this.sessionPropertyManager = checkNotNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.tablePropertyManager = checkNotNull(tablePropertyManager, "tablePropertyManager is null");
    }

    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    public synchronized void addConnectorMetadata(String connectorId, String catalogName, ConnectorMetadata connectorMetadata)
    {
        checkMetadataArguments(connectorId, catalogName, connectorMetadata);
        checkArgument(!connectorsByCatalog.containsKey(catalogName), "Catalog '%s' is already registered", catalogName);

        ConnectorMetadataEntry connectorMetadataEntry = new ConnectorMetadataEntry(connectorId, catalogName, connectorMetadata);
        connectorsById.put(connectorId, connectorMetadataEntry);
        connectorsByCatalog.put(catalogName, connectorMetadataEntry);
    }

    private void checkMetadataArguments(String connectorId, String catalogName, ConnectorMetadata metadata)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(metadata, "metadata is null");
        checkArgument(!connectorsById.containsKey(connectorId), "Connector '%s' is already registered", connectorId);
    }

    public Type getType(TypeSignature signature)
    {
        return typeManager.getType(signature);
    }

    public List<String> listSchemaNames(Session session, String catalogName)
    {
        checkCatalogName(catalogName);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (ConnectorMetadataEntry entry : allConnectorsFor(catalogName)) {
            schemaNames.addAll(entry.getMetadata().listSchemaNames(session.toConnectorSession(entry.getCatalog())));
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    public Optional<TableHandle> getTableHandle(Session session, QualifiedTableName table)
    {
        checkNotNull(table, "table is null");

        ConnectorMetadataEntry entry = getConnectorFor(table);
        if (entry != null) {
            ConnectorMetadata metadata = entry.getMetadata();

            ConnectorTableHandle tableHandle = metadata.getTableHandle(session.toConnectorSession(entry.getCatalog()), table.asSchemaTableName());

            if (tableHandle != null) {
                return Optional.of(new TableHandle(entry.getConnectorId(), tableHandle));
            }
        }
        return Optional.empty();
    }

    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        ConnectorTableMetadata tableMetadata = entry.getMetadata().getTableMetadata(
                session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());

        return new TableMetadata(tableHandle.getConnectorId(), tableMetadata);
    }

    public List<QualifiedTableName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        String schemaNameOrNull = prefix.getSchemaName().orElse(null);
        Set<QualifiedTableName> tables = new LinkedHashSet<>();
        for (ConnectorMetadataEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
            for (QualifiedTableName tableName : transform(entry.getMetadata().listTables(connectorSession, schemaNameOrNull), convertFromSchemaTableName(prefix.getCatalogName()))) {
                tables.add(tableName);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    public void createTable(Session session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);

        connectorMetadata.getMetadata().createTable(session.toConnectorSession(connectorMetadata.getCatalog()),
                tableMetadata.getMetadata());
    }

    public void renameTable(Session session, TableHandle tableHandle, QualifiedTableName newTableName)
    {
        String catalogName = newTableName.getCatalogName();
        ConnectorMetadataEntry target = connectorsByCatalog.get(catalogName);
        if (target == null) {
            throw new PrestoException(NOT_FOUND, format("Target catalog '%s' does not exist", catalogName));
        }
        if (!tableHandle.getConnectorId().equals(target.getConnectorId())) {
            throw new PrestoException(SYNTAX_ERROR, "Cannot rename tables across catalogs");
        }

        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().renameTable(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), newTableName.asSchemaTableName());
    }

    public void dropTable(Session session, TableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().dropTable(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());
    }


    public Map<String, String> getCatalogNames()
    {
        ImmutableMap.Builder<String, String> catalogsMap = ImmutableMap.builder();
        for (Map.Entry<String, ConnectorMetadataEntry> entry : connectorsByCatalog.entrySet()) {
            catalogsMap.put(entry.getKey(), entry.getValue().getConnectorId());
        }
        return catalogsMap.build();
    }

    public List<QualifiedTableName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        String schemaNameOrNull = prefix.getSchemaName().orElse(null);
        Set<QualifiedTableName> views = new LinkedHashSet<>();
        for (ConnectorMetadataEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
            for (QualifiedTableName tableName : transform(entry.getMetadata().listViews(connectorSession, schemaNameOrNull), convertFromSchemaTableName(prefix.getCatalogName()))) {
                views.add(tableName);
            }
        }
        return ImmutableList.copyOf(views);
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    public TablePropertyManager getTablePropertyManager()
    {
        return tablePropertyManager;
    }

    private List<ConnectorMetadataEntry> allConnectorsFor(String catalogName)
    {
        ImmutableList.Builder<ConnectorMetadataEntry> builder = ImmutableList.builder();

        ConnectorMetadataEntry connector = connectorsByCatalog.get(catalogName);
        if (connector != null) {
            builder.add(connector);
        }

        return builder.build();
    }

    private ConnectorMetadataEntry getConnectorFor(QualifiedTableName name)
    {
        String catalog = name.getCatalogName();
        String schema = name.getSchemaName();

        return connectorsByCatalog.get(catalog);
    }

    private ConnectorMetadataEntry lookupConnectorFor(TableHandle tableHandle)
    {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorMetadataEntry lookupConnectorFor(OutputTableHandle tableHandle)
    {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorMetadataEntry lookupConnectorFor(InsertTableHandle tableHandle)
    {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorMetadataEntry getConnectorMetadata(String connectorId)
    {
        ConnectorMetadataEntry result = connectorsById.get(connectorId);
        checkArgument(result != null, "No connector for connector ID: %s", connectorId);
        return result;
    }

    private static class ConnectorMetadataEntry
    {
        private final String connectorId;
        private final String catalog;
        private final ConnectorMetadata metadata;

        private ConnectorMetadataEntry(String connectorId, String catalog, ConnectorMetadata metadata)
        {
            this.connectorId = checkNotNull(connectorId, "connectorId is null");
            this.catalog = checkNotNull(catalog, "catalog is null");
            this.metadata = checkNotNull(metadata, "metadata is null");
        }

        private String getConnectorId()
        {
            return connectorId;
        }

        private String getCatalog()
        {
            return catalog;
        }

        private ConnectorMetadata getMetadata()
        {
            return metadata;
        }
    }

    private static JsonCodec<ViewDefinition> createTestingViewCodec()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new TypeDeserializer(new TypeRegistry())));
        return new JsonCodecFactory(provider).jsonCodec(ViewDefinition.class);
    }


    // *********************
    //
    // NETFLIX addition
    //
    // **********************

    public synchronized void flush(String catalogName){
        connectorsByCatalog.remove(catalogName);
        connectorsById.remove(catalogName);
    }

    public synchronized void flushAll(){
        connectorsByCatalog.clear();
        connectorsById.clear();
    }

    /**
     * Creates a schema with the given <code>schemaName</code>
     * @param session connector session
     * @param schemaMetadata schema metadata
     */
    public void createSchema(Session session, ConnectorSchemaMetadata schemaMetadata) {
        String schemaName = session.getSchema();
        checkArgument(schemaName != null, "Schema cannot be null");
        ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        ConnectorMetadata metadata = entry.getMetadata();
        if (!(metadata instanceof ConnectorDetailMetadata)) {
            throw new PrestoException(NOT_SUPPORTED,
                    "Create schema not supported for connector " + entry.getConnectorId());
        }

        ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
        detailMetadata.createSchema(session.toConnectorSession(), schemaMetadata);
    }

    /**
     * Updates a schema with the given <code>schemaName</code>
     * @param session connector session
     * @param schemaMetadata schema metadata
     */
    public void updateSchema(Session session, ConnectorSchemaMetadata schemaMetadata) {
        String schemaName = session.getSchema();
        checkArgument(schemaName != null, "Schema cannot be null");
        ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        ConnectorMetadata metadata = entry.getMetadata();
        if (!(metadata instanceof ConnectorDetailMetadata)) {
            throw new PrestoException(NOT_SUPPORTED,
                    "Update schema not supported for connector " + entry.getConnectorId());
        }

        ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
        detailMetadata.updateSchema(session.toConnectorSession(), schemaMetadata);
    }

    /**
     * Drop a schema with the given <code>schemaName</code>
     * @param session connector session
     */
    public void dropSchema(Session session) {
        String schemaName = session.getSchema();
        checkArgument(schemaName != null, "Schema cannot be null");
        ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        ConnectorMetadata metadata = entry.getMetadata();
        if (!(metadata instanceof ConnectorDetailMetadata)) {
            throw new PrestoException(NOT_SUPPORTED,
                    "Drop schema not supported for connector " + entry.getConnectorId());
        }

        ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
        detailMetadata.dropSchema(session.toConnectorSession(), schemaName);
    }

    /**
     * Return a schema with the given <code>schemaName</code>
     * @param session connector session
     * @return  Return a schema with the given schemaName
     */
    public ConnectorSchemaMetadata getSchema(Session session) {
        String schemaName = session.getSchema();
        checkArgument(schemaName != null, "Schema cannot be null");
        ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());
        ConnectorSchemaMetadata result;
        ConnectorMetadata metadata = entry.getMetadata();
        if (metadata instanceof ConnectorDetailMetadata) {
            result = ((ConnectorDetailMetadata) metadata).getSchema(session.toConnectorSession(), schemaName);
        } else {
            result = new ConnectorSchemaMetadata(schemaName);
        }

        return result;
    }

    /**
     * Updates a table using the specified table metadata.
     * @param session connector session
     * @param tableMetadata table metadata
     * @return table handle
     */
    public ConnectorTableHandle alterTable(Session session, TableMetadata tableMetadata) {
        ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        ConnectorMetadata metadata = entry.getMetadata();
        if (!(metadata instanceof ConnectorDetailMetadata)) {
            throw new PrestoException(NOT_SUPPORTED,
                    "Alter table not supported for connector " + entry.getConnectorId());
        }

        ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
        return detailMetadata.alterTable(session.toConnectorSession(), tableMetadata.getMetadata());
    }

    public List<SchemaTableName> getTableNames(Session session, String uri, boolean prefixSearch){
        ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        ConnectorMetadata metadata = entry.getMetadata();
        if( metadata instanceof ConnectorDetailMetadata){
            ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
            return detailMetadata.getTableNames(uri, prefixSearch);
        }
        return Lists.newArrayList();
    }

    public List<TableMetadata> listTableMetadatas(Session session, String schemaName, List<String> tableNames){
        ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());
        ConnectorMetadata metadata = entry.getMetadata();
        if( metadata instanceof ConnectorDetailMetadata){
            List<TableMetadata> result = Lists.newArrayList();
            ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
            List<ConnectorTableMetadata> cdm = detailMetadata
                    .listTableMetadatas(session.toConnectorSession(), schemaName, tableNames);
            if( cdm != null){

                cdm.forEach(
                        connectorTableMetadata -> result.add( new TableMetadata(session.getCatalog(), connectorTableMetadata)));
            }
            return result;
        } else {
            return tableNames.stream().map(tableName -> {
                TableMetadata result = null;
                Optional<TableHandle> tableHandle = getTableHandle(session, new QualifiedTableName(session.getCatalog(), schemaName, tableName));
                if( tableHandle.isPresent()){
                    result = getTableMetadata(session, tableHandle.get());
                }
                return result;
            }).filter(tableMetadata -> tableMetadata != null).collect(Collectors.toList());
        }
    }

    public ConnectorMetadataEntry validateCatalogName(String catalogName) {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        if (connectorMetadata == null) {
            throw new CatalogNotFoundException(catalogName);
        }

        return connectorMetadata;
    }
}
