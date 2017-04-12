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
import com.facebook.presto.metadata.MetadataUtil;
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
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

/**
 * Metadata manager.
 */
public class MetadataManager {
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectorsByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectorsById = new ConcurrentHashMap<>();
    private final TypeManager typeManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final TablePropertyManager tablePropertyManager;

    /**
     * Constructor.
     * @param typeManager type manager
     * @param sessionPropertyManager session manager
     * @param tablePropertyManager table manager
     */
    @Inject
    public MetadataManager(
        final TypeManager typeManager,
        final SessionPropertyManager sessionPropertyManager,
        final TablePropertyManager tablePropertyManager) {
        this.typeManager = Preconditions.checkNotNull(typeManager, "types is null");
        this.sessionPropertyManager =
            Preconditions.checkNotNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.tablePropertyManager = Preconditions.checkNotNull(tablePropertyManager, "tablePropertyManager is null");
    }

    public SessionPropertyManager getSessionPropertyManager() {
        return sessionPropertyManager;
    }

    /**
     * Add connector metadata.
     * @param connectorId connector id
     * @param catalogName catalog name
     * @param connectorMetadata connector metadata
     */
    public synchronized void addConnectorMetadata(final String connectorId, final String catalogName,
        final ConnectorMetadata connectorMetadata) {
        checkMetadataArguments(connectorId, catalogName, connectorMetadata);
        Preconditions.checkArgument(!connectorsByCatalog.containsKey(catalogName),
            "Catalog '%s' is already registered", catalogName);

        final ConnectorMetadataEntry connectorMetadataEntry = new ConnectorMetadataEntry(connectorId, catalogName,
            connectorMetadata);
        connectorsById.put(connectorId, connectorMetadataEntry);
        connectorsByCatalog.put(catalogName, connectorMetadataEntry);
    }

    private void checkMetadataArguments(final String connectorId, final String catalogName,
        final ConnectorMetadata metadata) {
        Preconditions.checkNotNull(connectorId, "connectorId is null");
        Preconditions.checkNotNull(catalogName, "catalogName is null");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkArgument(!connectorsById.containsKey(connectorId),
            "Connector '%s' is already registered", connectorId);
    }

    /**
     * Get the type.
     * @param signature type signature
     * @return type
     */
    public Type getType(final TypeSignature signature) {
        return typeManager.getType(signature);
    }

    /**
     * Lists schema names.
     * @param session session
     * @param catalogName catalog name
     * @return schema names
     */
    public List<String> listSchemaNames(final Session session, final String catalogName) {
        MetadataUtil.checkCatalogName(catalogName);
        final ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (ConnectorMetadataEntry entry : allConnectorsFor(catalogName)) {
            schemaNames.addAll(entry.getMetadata().listSchemaNames(session.toConnectorSession(entry.getCatalog())));
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    /**
     * Get the table handle fo the given table.
     * @param session session
     * @param table table
     * @return table handle
     */
    public Optional<TableHandle> getTableHandle(final Session session, final QualifiedTableName table) {
        Preconditions.checkNotNull(table, "table is null");

        final ConnectorMetadataEntry entry = getConnectorFor(table);
        if (entry != null) {
            final ConnectorMetadata metadata = entry.getMetadata();

            final ConnectorTableHandle tableHandle = metadata
                .getTableHandle(session.toConnectorSession(entry.getCatalog()), table.asSchemaTableName());

            if (tableHandle != null) {
                return Optional.of(new TableHandle(entry.getConnectorId(), tableHandle));
            }
        }
        return Optional.empty();
    }

    /**
     * Get the table info.
     * @param session session
     * @param tableHandle table handle
     * @return table info
     */
    public TableMetadata getTableMetadata(final Session session, final TableHandle tableHandle) {
        final ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        final ConnectorTableMetadata tableMetadata = entry.getMetadata().getTableMetadata(
            session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());

        return new TableMetadata(tableHandle.getConnectorId(), tableMetadata);
    }

    /**
     * Returns the list of table names that matches the given prefix.
     * @param session session
     * @param prefix table name prefix
     * @return list of table names
     */
    public List<QualifiedTableName> listTables(final Session session, final QualifiedTablePrefix prefix) {
        Preconditions.checkNotNull(prefix, "prefix is null");

        final String schemaNameOrNull = prefix.getSchemaName().orElse(null);
        final Set<QualifiedTableName> tables = new LinkedHashSet<>();
        for (ConnectorMetadataEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            final ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
            for (QualifiedTableName tableName : Iterables.transform(
                entry.getMetadata().listTables(connectorSession, schemaNameOrNull),
                QualifiedTableName.convertFromSchemaTableName(prefix.getCatalogName()))) {
                tables.add(tableName);
            }
        }
        return ImmutableList.copyOf(tables);
    }

    /**
     * Create table.
     * @param session session
     * @param catalogName catalog name
     * @param tableMetadata table info
     */
    public void createTable(final Session session, final String catalogName, final TableMetadata tableMetadata) {
        final ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        Preconditions.checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);

        connectorMetadata.getMetadata().createTable(session.toConnectorSession(connectorMetadata.getCatalog()),
            tableMetadata.getMetadata());
    }

    /**
     * Rename table.
     * @param session session
     * @param tableHandle handle
     * @param newTableName new table name
     */
    public void renameTable(final Session session, final TableHandle tableHandle,
        final QualifiedTableName newTableName) {
        final String catalogName = newTableName.getCatalogName();
        final ConnectorMetadataEntry target = connectorsByCatalog.get(catalogName);
        if (target == null) {
            throw new PrestoException(
                StandardErrorCode.NOT_FOUND, String.format("Target catalog '%s' does not exist", catalogName));
        }
        if (!tableHandle.getConnectorId().equals(target.getConnectorId())) {
            throw new PrestoException(StandardErrorCode.SYNTAX_ERROR, "Cannot rename tables across catalogs");
        }

        final ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata()
            .renameTable(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(),
                newTableName.asSchemaTableName());
    }

    /**
     * Drop table.
     * @param session session
     * @param tableHandle table
     */
    public void dropTable(final Session session, final TableHandle tableHandle) {
        final ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().dropTable(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());
    }

    /**
     * Get catalog names.
     * @return Map of catalog names
     */
    public Map<String, String> getCatalogNames() {
        final ImmutableMap.Builder<String, String> catalogsMap = ImmutableMap.builder();
        for (Map.Entry<String, ConnectorMetadataEntry> entry : connectorsByCatalog.entrySet()) {
            catalogsMap.put(entry.getKey(), entry.getValue().getConnectorId());
        }
        return catalogsMap.build();
    }

    /**
     * List of view names.
     * @param session session
     * @param prefix prefix
     * @return list of view names.
     */
    public List<QualifiedTableName> listViews(final Session session, final QualifiedTablePrefix prefix) {
        Preconditions.checkNotNull(prefix, "prefix is null");

        final String schemaNameOrNull = prefix.getSchemaName().orElse(null);
        final Set<QualifiedTableName> views = new LinkedHashSet<>();
        for (ConnectorMetadataEntry entry : allConnectorsFor(prefix.getCatalogName())) {
            final ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
            for (QualifiedTableName tableName : Iterables.transform(
                entry.getMetadata().listViews(connectorSession, schemaNameOrNull),
                QualifiedTableName.convertFromSchemaTableName(prefix.getCatalogName()))) {
                views.add(tableName);
            }
        }
        return ImmutableList.copyOf(views);
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }

    public TablePropertyManager getTablePropertyManager() {
        return tablePropertyManager;
    }

    private List<ConnectorMetadataEntry> allConnectorsFor(final String catalogName) {
        final ImmutableList.Builder<ConnectorMetadataEntry> builder = ImmutableList.builder();

        final ConnectorMetadataEntry connector = connectorsByCatalog.get(catalogName);
        if (connector != null) {
            builder.add(connector);
        }

        return builder.build();
    }

    private ConnectorMetadataEntry getConnectorFor(final QualifiedTableName name) {
        final String catalog = name.getCatalogName();

        return connectorsByCatalog.get(catalog);
    }

    private ConnectorMetadataEntry lookupConnectorFor(final TableHandle tableHandle) {
        return getConnectorMetadata(tableHandle.getConnectorId());
    }

    private ConnectorMetadataEntry getConnectorMetadata(final String connectorId) {
        final ConnectorMetadataEntry result = connectorsById.get(connectorId);
        Preconditions.checkArgument(result != null, "No connector for connector ID: %s", connectorId);
        return result;
    }

    private static final class ConnectorMetadataEntry {
        private final String connectorId;
        private final String catalog;
        private final ConnectorMetadata metadata;

        private ConnectorMetadataEntry(final String connectorId, final String catalog,
            final ConnectorMetadata metadata) {
            this.connectorId = Preconditions.checkNotNull(connectorId, "connectorId is null");
            this.catalog = Preconditions.checkNotNull(catalog, "catalog is null");
            this.metadata = Preconditions.checkNotNull(metadata, "metadata is null");
        }

        private String getConnectorId() {
            return connectorId;
        }

        private String getCatalog() {
            return catalog;
        }

        private ConnectorMetadata getMetadata() {
            return metadata;
        }
    }

    private static JsonCodec<ViewDefinition> createTestingViewCodec() {
        final ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(
            ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new TypeDeserializer(new TypeRegistry())));
        return new JsonCodecFactory(provider).jsonCodec(ViewDefinition.class);
    }

    // *********************
    //
    // NETFLIX addition
    //
    // **********************

    /**
     * Flushes the catalog.
     * @param catalogName catalog name.
     */
    public synchronized void flush(final String catalogName) {
        connectorsByCatalog.remove(catalogName);
        connectorsById.remove(catalogName);
    }

    /**
     * Flushes all catalogs.
     */
    public synchronized void flushAll() {
        connectorsByCatalog.clear();
        connectorsById.clear();
    }

    /**
     * Creates a schema with the given <code>schemaName</code>.
     * @param session connector session
     * @param schemaMetadata schema metadata
     */
    public void createSchema(final Session session, final ConnectorSchemaMetadata schemaMetadata) {
        final String schemaName = session.getSchema();
        Preconditions.checkArgument(schemaName != null, "Schema cannot be null");
        final ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        final ConnectorMetadata metadata = entry.getMetadata();
        if (!(metadata instanceof ConnectorDetailMetadata)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                "Create schema not supported for connector " + entry.getConnectorId());
        }

        final ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
        detailMetadata.createSchema(session.toConnectorSession(), schemaMetadata);
    }

    /**
     * Updates a schema with the given <code>schemaName</code>.
     * @param session connector session
     * @param schemaMetadata schema metadata
     */
    public void updateSchema(final Session session, final ConnectorSchemaMetadata schemaMetadata) {
        final String schemaName = session.getSchema();
        Preconditions.checkArgument(schemaName != null, "Schema cannot be null");
        final ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        final ConnectorMetadata metadata = entry.getMetadata();
        if (!(metadata instanceof ConnectorDetailMetadata)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                "Update schema not supported for connector " + entry.getConnectorId());
        }

        final ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
        detailMetadata.updateSchema(session.toConnectorSession(), schemaMetadata);
    }

    /**
     * Drop a schema with the given <code>schemaName</code>.
     * @param session connector session
     */
    public void dropSchema(final Session session) {
        final String schemaName = session.getSchema();
        Preconditions.checkArgument(schemaName != null, "Schema cannot be null");
        final ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        final ConnectorMetadata metadata = entry.getMetadata();
        if (!(metadata instanceof ConnectorDetailMetadata)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                "Drop schema not supported for connector " + entry.getConnectorId());
        }

        final ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
        detailMetadata.dropSchema(session.toConnectorSession(), schemaName);
    }

    /**
     * Return a schema with the given <code>schemaName</code>.
     * @param session connector session
     * @return Return a schema with the given schemaName
     */
    public ConnectorSchemaMetadata getSchema(final Session session) {
        final String schemaName = session.getSchema();
        Preconditions.checkArgument(schemaName != null, "Schema cannot be null");
        final ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());
        final ConnectorSchemaMetadata result;
        final ConnectorMetadata metadata = entry.getMetadata();
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
    public ConnectorTableHandle alterTable(final Session session, final TableMetadata tableMetadata) {
        final ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        final ConnectorMetadata metadata = entry.getMetadata();
        if (!(metadata instanceof ConnectorDetailMetadata)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED,
                "Alter table not supported for connector " + entry.getConnectorId());
        }

        final ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
        return detailMetadata.alterTable(session.toConnectorSession(), tableMetadata.getMetadata());
    }

    /**
     * List table names.
     * @param session session
     * @param uri uri
     * @param prefixSearch prefix search
     * @return list of table names
     */
    public List<SchemaTableName> getTableNames(final Session session, final String uri, final boolean prefixSearch) {
        final ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        final ConnectorMetadata metadata = entry.getMetadata();
        if (metadata instanceof ConnectorDetailMetadata) {
            final ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
            return detailMetadata.getTableNames(uri, prefixSearch);
        }
        return Lists.newArrayList();
    }

    /**
     * List table names for the given uris.
     * @param session session
     * @param uris uris
     * @param prefixSearch prefix search
     * @return list of table names
     */
    public Map<String, List<SchemaTableName>> getTableNames(final Session session, final List<String> uris,
        final boolean prefixSearch) {
        final ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());

        final ConnectorMetadata metadata = entry.getMetadata();
        if (metadata instanceof ConnectorDetailMetadata) {
            final ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
            return detailMetadata.getTableNames(uris, prefixSearch);
        }
        return Maps.newHashMap();
    }

    /**
     * List tables.
     * @param session session
     * @param schemaName schema name
     * @param tableNames list of table names
     * @return list of tables.
     */
    public List<TableMetadata> listTableMetadatas(final Session session, final String schemaName,
        final List<String> tableNames) {
        final ConnectorMetadataEntry entry = validateCatalogName(session.getCatalog());
        final ConnectorMetadata metadata = entry.getMetadata();
        if (metadata instanceof ConnectorDetailMetadata) {
            final List<TableMetadata> result = Lists.newArrayList();
            final ConnectorDetailMetadata detailMetadata = (ConnectorDetailMetadata) metadata;
            final List<ConnectorTableMetadata> cdm = detailMetadata
                .listTableMetadatas(session.toConnectorSession(), schemaName, tableNames);
            if (cdm != null) {

                cdm.forEach(
                    connectorTableMetadata -> result
                        .add(new TableMetadata(session.getCatalog(), connectorTableMetadata)));
            }
            return result;
        } else {
            return tableNames.stream().map(tableName -> {
                TableMetadata result = null;
                final Optional<TableHandle> tableHandle = getTableHandle(session,
                    new QualifiedTableName(session.getCatalog(), schemaName, tableName));
                if (tableHandle.isPresent()) {
                    result = getTableMetadata(session, tableHandle.get());
                }
                return result;
            }).filter(tableMetadata -> tableMetadata != null).collect(Collectors.toList());
        }
    }

    /**
     * Validates the catalog name.
     * @param catalogName catalog name
     * @return ConnectorMetadataEntry
     */
    public ConnectorMetadataEntry validateCatalogName(final String catalogName) {
        final ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        if (connectorMetadata == null) {
            throw new CatalogNotFoundException(catalogName);
        }

        return connectorMetadata;
    }
}
