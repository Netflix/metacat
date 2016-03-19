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
import com.facebook.presto.connector.informationSchema.InformationSchemaMetadata;
import com.facebook.presto.exception.CatalogNotFoundException;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.LegacyTableLayoutHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TablePropertyManager;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorDetailMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSchemaMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.QualifiedName;
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
import io.airlift.slice.Slice;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.QualifiedTableName.convertFromSchemaTableName;
import static com.facebook.presto.metadata.TableLayout.fromConnectorLayout;
import static com.facebook.presto.metadata.ViewDefinition.ViewColumn;
import static com.facebook.presto.spi.StandardErrorCode.*;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class MetadataManager
        implements Metadata
{
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final ConcurrentMap<String, ConnectorMetadataEntry> informationSchemasByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> systemTablesByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectorsByCatalog = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadataEntry> connectorsById = new ConcurrentHashMap<>();
    private final TypeManager typeManager;
    private final JsonCodec<ViewDefinition> viewCodec;
    private final SplitManager splitManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final TablePropertyManager tablePropertyManager;

    public MetadataManager(FeaturesConfig featuresConfig,
            TypeManager typeManager,
            SplitManager splitManager,
            SessionPropertyManager sessionPropertyManager,
            TablePropertyManager tablePropertyManager)
    {
        this(featuresConfig, typeManager, createTestingViewCodec(), splitManager,
                sessionPropertyManager, tablePropertyManager);
    }

    @Inject
    public MetadataManager(FeaturesConfig featuresConfig,
            TypeManager typeManager,
            JsonCodec<ViewDefinition> viewCodec,
            SplitManager splitManager,
            SessionPropertyManager sessionPropertyManager,
            TablePropertyManager tablePropertyManager)
    {
        this.typeManager = checkNotNull(typeManager, "types is null");
        this.viewCodec = checkNotNull(viewCodec, "viewCodec is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.sessionPropertyManager = checkNotNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.tablePropertyManager = checkNotNull(tablePropertyManager, "tablePropertyManager is null");
    }

    public static MetadataManager createTestMetadataManager()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        TypeManager typeManager = new TypeRegistry();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        SplitManager splitManager = new SplitManager();
        return new MetadataManager(featuresConfig, typeManager, splitManager, sessionPropertyManager, new TablePropertyManager());
    }

    public synchronized void addConnectorMetadata(String connectorId, String catalogName, ConnectorMetadata connectorMetadata)
    {
        checkMetadataArguments(connectorId, catalogName, connectorMetadata);
        checkArgument(!connectorsByCatalog.containsKey(catalogName), "Catalog '%s' is already registered", catalogName);

        ConnectorMetadataEntry connectorMetadataEntry = new ConnectorMetadataEntry(connectorId, catalogName, connectorMetadata);
        connectorsById.put(connectorId, connectorMetadataEntry);
        connectorsByCatalog.put(catalogName, connectorMetadataEntry);
    }

    public synchronized void addInformationSchemaMetadata(String connectorId, String catalogName, InformationSchemaMetadata metadata)
    {
        checkMetadataArguments(connectorId, catalogName, metadata);
        checkArgument(!informationSchemasByCatalog.containsKey(catalogName), "Information schema for catalog '%s' is already registered", catalogName);

        ConnectorMetadataEntry connectorMetadataEntry = new ConnectorMetadataEntry(connectorId, catalogName, metadata);
        connectorsById.put(connectorId, connectorMetadataEntry);
        informationSchemasByCatalog.put(catalogName, connectorMetadataEntry);
    }

    public synchronized void addSystemTablesMetadata(String connectorId, String catalogName, ConnectorMetadata metadata)
    {
        checkMetadataArguments(connectorId, catalogName, metadata);
        checkArgument(!systemTablesByCatalog.containsKey(catalogName), "System tables for catalog '%s' are already registered", catalogName);

        ConnectorMetadataEntry connectorMetadataEntry = new ConnectorMetadataEntry(connectorId, catalogName, metadata);
        connectorsById.put(connectorId, connectorMetadataEntry);
        systemTablesByCatalog.put(catalogName, connectorMetadataEntry);
    }

    private void checkMetadataArguments(String connectorId, String catalogName, ConnectorMetadata metadata)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(metadata, "metadata is null");
        checkArgument(!connectorsById.containsKey(connectorId), "Connector '%s' is already registered", connectorId);
    }

    @Override
    public final void verifyComparableOrderableContract()
    {
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return typeManager.getType(signature);
    }

    @Override
    public FunctionInfo resolveFunction(QualifiedName name, List<TypeSignature> parameterTypes, boolean approximate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionInfo getExactFunction(Signature handle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ParametricFunction> listFunctions()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addFunctions(List<? extends ParametricFunction> functionInfos)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FunctionInfo resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        checkCatalogName(catalogName);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (ConnectorMetadataEntry entry : allConnectorsFor(catalogName)) {
            schemaNames.addAll(entry.getMetadata().listSchemaNames(session.toConnectorSession(entry.getCatalog())));
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
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

    @Override
    public List<TableLayoutResult> getLayouts(Session session, TableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        if (constraint.getSummary().isNone()) {
            return ImmutableList.of();
        }

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        String connectorId = table.getConnectorId();
        ConnectorTableHandle connectorTable = table.getConnectorHandle();
        Predicate<Map<ColumnHandle, ?>> predicate = constraint.predicate();

        List<ConnectorTableLayoutResult> layouts;
        ConnectorMetadataEntry entry = getConnectorMetadata(connectorId);
        ConnectorSession connectorSession = session.toConnectorSession(entry.getCatalog());
        try {
            layouts = entry.getMetadata().getTableLayouts(connectorSession, connectorTable, new Constraint<>(summary, predicate::test), desiredColumns);
        }
        catch (UnsupportedOperationException e) {
            ConnectorSplitManager connectorSplitManager = splitManager.getConnectorSplitManager(connectorId);
            ConnectorPartitionResult result = connectorSplitManager.getPartitions(connectorSession, connectorTable, summary);

            List<ConnectorPartition> partitions = result.getPartitions().stream()
                    .filter(partition -> predicate.test(partition.getTupleDomain().extractFixedValues()))
                    .collect(toImmutableList());

            List<TupleDomain<ColumnHandle>> partitionDomains = partitions.stream()
                    .map(ConnectorPartition::getTupleDomain)
                    .collect(toImmutableList());

            TupleDomain<ColumnHandle> effectivePredicate = TupleDomain.none();
            if (!partitionDomains.isEmpty()) {
                effectivePredicate = TupleDomain.columnWiseUnion(partitionDomains);
            }

            ConnectorTableLayout layout = new ConnectorTableLayout(new LegacyTableLayoutHandle(connectorTable, partitions), Optional.empty(), effectivePredicate, Optional.empty(), Optional.of(partitionDomains), ImmutableList.of());
            layouts = ImmutableList.of(new ConnectorTableLayoutResult(layout, result.getUndeterminedTupleDomain()));
        }

        return layouts.stream()
                .map(layout -> new TableLayoutResult(fromConnectorLayout(connectorId, layout.getTableLayout()), layout.getUnenforcedConstraint()))
                .collect(toImmutableList());
    }

    @Override
    public TableLayout getLayout(Session session, TableLayoutHandle handle)
    {
        if (handle.getConnectorHandle() instanceof LegacyTableLayoutHandle) {
            LegacyTableLayoutHandle legacyHandle = (LegacyTableLayoutHandle) handle.getConnectorHandle();
            List<TupleDomain<ColumnHandle>> partitionDomains = legacyHandle.getPartitions().stream()
                    .map(ConnectorPartition::getTupleDomain)
                    .collect(toImmutableList());

            TupleDomain<ColumnHandle> predicate = TupleDomain.none();
            if (!partitionDomains.isEmpty()) {
                predicate = TupleDomain.columnWiseUnion(partitionDomains);
            }
            return new TableLayout(handle, new ConnectorTableLayout(legacyHandle, Optional.empty(), predicate, Optional.empty(), Optional.of(partitionDomains), ImmutableList.of()));
        }

        String connectorId = handle.getConnectorId();
        ConnectorMetadataEntry entry = getConnectorMetadata(connectorId);
        return fromConnectorLayout(connectorId, entry.getMetadata()
                .getTableLayout(session.toConnectorSession(entry.getCatalog()), handle.getConnectorHandle()));
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        ConnectorTableMetadata tableMetadata = entry.getMetadata().getTableMetadata(
                session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());

        return new TableMetadata(tableHandle.getConnectorId(), tableMetadata);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        return entry.getMetadata().getColumnHandles(session.toConnectorSession(entry.getCatalog()),
                tableHandle.getConnectorHandle());
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");

        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        return entry.getMetadata().getColumnMetadata(session.toConnectorSession(entry.getCatalog()),
                tableHandle.getConnectorHandle(), columnHandle);
    }

    @Override
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

    @Override
    public Optional<ColumnHandle> getSampleWeightColumnHandle(Session session, TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        ColumnHandle handle = entry.getMetadata().getSampleWeightColumnHandle(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());

        return Optional.ofNullable(handle);
    }

    @Override
    public boolean canCreateSampledTables(Session session, String catalogName)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.getMetadata().canCreateSampledTables(session.toConnectorSession(connectorMetadata.getCatalog()));
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();

        Map<QualifiedTableName, List<ColumnMetadata>> tableColumns = new HashMap<>();
        for (ConnectorMetadataEntry connectorMetadata : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorMetadata metadata = connectorMetadata.getMetadata();

            ConnectorSession connectorSession = session.toConnectorSession(connectorMetadata.getCatalog());
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : metadata.listTableColumns(connectorSession, tablePrefix).entrySet()) {
                QualifiedTableName tableName = new QualifiedTableName(
                        prefix.getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getTableName());
                tableColumns.put(tableName, entry.getValue());
            }

            // if table and view names overlap, the view wins
            for (Entry<SchemaTableName, String> entry : metadata.getViews(connectorSession, tablePrefix).entrySet()) {
                QualifiedTableName tableName = new QualifiedTableName(
                        prefix.getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getTableName());

                ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
                for (ViewColumn column : deserializeView(entry.getValue()).getColumns()) {
                    columns.add(new ColumnMetadata(column.getName(), column.getType(), false));
                }

                tableColumns.put(tableName, columns.build());
            }
        }
        return ImmutableMap.copyOf(tableColumns);
    }

    @Override
    public void createTable(Session session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);

        connectorMetadata.getMetadata().createTable(session.toConnectorSession(connectorMetadata.getCatalog()),
                tableMetadata.getMetadata());
    }

    @Override
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

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, ColumnHandle source, String target)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().renameColumn(session.toConnectorSession(entry.getCatalog()),
                tableHandle.getConnectorHandle(), source, target.toLowerCase(ENGLISH));
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().dropTable(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        ConnectorSession connectorSession = session.toConnectorSession(connectorMetadata.getCatalog());
        ConnectorOutputTableHandle handle = connectorMetadata.getMetadata().beginCreateTable(connectorSession, tableMetadata.getMetadata());
        return new OutputTableHandle(connectorMetadata.getConnectorId(), handle);
    }

    @Override
    public void commitCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().commitCreateTable(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public void rollbackCreateTable(Session session, OutputTableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().rollbackCreateTable(session.toConnectorSession(entry.getCatalog()),
                tableHandle.getConnectorHandle());
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        ConnectorInsertTableHandle handle = entry.getMetadata().beginInsert(
                session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());
        return new InsertTableHandle(tableHandle.getConnectorId(), handle);
    }

    @Override
    public void commitInsert(Session session, InsertTableHandle tableHandle, Collection<Slice> fragments)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().commitInsert(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public void rollbackInsert(Session session, InsertTableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().rollbackInsert(session.toConnectorSession(entry.getCatalog()), tableHandle.getConnectorHandle());
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        return entry.getMetadata().getUpdateRowIdColumnHandle(session.toConnectorSession(entry.getCatalog()),
                tableHandle.getConnectorHandle());
    }

    @Override
    public TableHandle beginDelete(Session session, TableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        ConnectorTableHandle newHandle = entry.getMetadata().beginDelete(session.toConnectorSession(entry.getCatalog()),
                tableHandle.getConnectorHandle());
        return new TableHandle(tableHandle.getConnectorId(), newHandle);
    }

    @Override
    public void commitDelete(Session session, TableHandle tableHandle, Collection<Slice> fragments)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().commitDelete(session.toConnectorSession(entry.getCatalog()),
                tableHandle.getConnectorHandle(), fragments);
    }

    @Override
    public void rollbackDelete(Session session, TableHandle tableHandle)
    {
        ConnectorMetadataEntry entry = lookupConnectorFor(tableHandle);
        entry.getMetadata().rollbackDelete(session.toConnectorSession(entry.getCatalog()),
                tableHandle.getConnectorHandle());
    }

    @Override
    public Map<String, String> getCatalogNames()
    {
        ImmutableMap.Builder<String, String> catalogsMap = ImmutableMap.builder();
        for (Map.Entry<String, ConnectorMetadataEntry> entry : connectorsByCatalog.entrySet()) {
            catalogsMap.put(entry.getKey(), entry.getValue().getConnectorId());
        }
        return catalogsMap.build();
    }

    @Override
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

    @Override
    public Map<QualifiedTableName, ViewDefinition> getViews(Session session, QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        SchemaTablePrefix tablePrefix = prefix.asSchemaTablePrefix();

        Map<QualifiedTableName, ViewDefinition> views = new LinkedHashMap<>();
        for (ConnectorMetadataEntry metadata : allConnectorsFor(prefix.getCatalogName())) {
            ConnectorSession connectorSession = session.toConnectorSession(metadata.getCatalog());
            for (Entry<SchemaTableName, String> entry : metadata.getMetadata().getViews(connectorSession, tablePrefix).entrySet()) {
                QualifiedTableName viewName = new QualifiedTableName(
                        prefix.getCatalogName(),
                        entry.getKey().getSchemaName(),
                        entry.getKey().getTableName());
                views.put(viewName, deserializeView(entry.getValue()));
            }
        }
        return ImmutableMap.copyOf(views);
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedTableName viewName)
    {
        ConnectorMetadataEntry entry = getConnectorFor(viewName);
        if (entry != null) {
            SchemaTablePrefix prefix = viewName.asSchemaTableName().toSchemaTablePrefix();
            Map<SchemaTableName, String> views = entry.getMetadata().getViews(session.toConnectorSession(entry.getCatalog()), prefix);
            String view = views.get(viewName.asSchemaTableName());
            if (view != null) {
                return Optional.of(deserializeView(view));
            }
        }
        return Optional.empty();
    }

    @Override
    public void createView(Session session, QualifiedTableName viewName, String viewData, boolean replace)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(viewName.getCatalogName());
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", viewName.getCatalogName());
        connectorMetadata.getMetadata().createView(session.toConnectorSession(connectorMetadata.getCatalog()), viewName.asSchemaTableName(), viewData, replace);
    }

    @Override
    public void dropView(Session session, QualifiedTableName viewName)
    {
        ConnectorMetadataEntry connectorMetadata = connectorsByCatalog.get(viewName.getCatalogName());
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", viewName.getCatalogName());
        connectorMetadata.getMetadata().dropView(session.toConnectorSession(connectorMetadata.getCatalog()),
                viewName.asSchemaTableName());
    }

    @Override
    public FunctionRegistry getFunctionRegistry()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    @Override
    public BlockEncodingSerde getBlockEncodingSerde()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SessionPropertyManager getSessionPropertyManager()
    {
        return sessionPropertyManager;
    }

    @Override
    public TablePropertyManager getTablePropertyManager()
    {
        return tablePropertyManager;
    }

    private ViewDefinition deserializeView(String data)
    {
        try {
            return viewCodec.fromJson(data);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_VIEW, "Invalid view JSON: " + data, e);
        }
    }

    private List<ConnectorMetadataEntry> allConnectorsFor(String catalogName)
    {
        ImmutableList.Builder<ConnectorMetadataEntry> builder = ImmutableList.builder();

        ConnectorMetadataEntry entry = informationSchemasByCatalog.get(catalogName);
        if (entry != null) {
            builder.add(entry);
        }

        ConnectorMetadataEntry systemTables = systemTablesByCatalog.get(catalogName);
        if (systemTables != null) {
            builder.add(systemTables);
        }

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

        if (schema.equals(INFORMATION_SCHEMA_NAME)) {
            return informationSchemasByCatalog.get(catalog);
        }

        ConnectorMetadataEntry entry = systemTablesByCatalog.get(catalog);
        if ((entry != null) && (entry.getMetadata().getTableHandle(null, name.asSchemaTableName()) != null)) {
            return entry;
        }

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


    //////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * NETFLIX addition
     */
    public synchronized void flush(String catalogName){
        informationSchemasByCatalog.remove(catalogName);
        connectorsByCatalog.remove(catalogName);
        connectorsById.remove(catalogName);
    }

    /**
     * NETFLIX addition
     */
    public synchronized void flushAll(){
        informationSchemasByCatalog.clear();
        connectorsByCatalog.clear();
        connectorsById.clear();
    }

    /**
     * Creates a schema with the given <code>schemaName</code>
     * @param session connector session
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
