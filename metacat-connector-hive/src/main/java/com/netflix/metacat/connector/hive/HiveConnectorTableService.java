/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.exception.ConnectorException;
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException;
import com.netflix.metacat.common.server.exception.InvalidMetaException;
import com.netflix.metacat.common.server.exception.TableNotFoundException;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;

/**
 * Hive base connector base service impl.
 *
 * @author zhenl
 */
public class HiveConnectorTableService implements ConnectorTableService {
    private static final String PARAMETER_EXTERNAL = "EXTERNAL";
    private final MetacatHiveClient metacatHiveClient;
    private final HiveConnectorInfoConverter hiveMetacatConverters;
    private final HiveConnectorDatabaseService hiveConnectorDatabaseService;
    private final String catalogName;

    /**
     * Constructor.
     *
     * @param catalogName                  catalogname
     * @param metacatHiveClient            hiveclient
     * @param hiveConnectorDatabaseService hivedatabaseService
     * @param hiveMetacatConverters        converter
     */
    @Inject
    public HiveConnectorTableService(@Named("catalogName") final String catalogName,
                                     @Nonnull final MetacatHiveClient metacatHiveClient,
                                     @Nonnull final HiveConnectorDatabaseService hiveConnectorDatabaseService,
                                     @Nonnull final HiveConnectorInfoConverter hiveMetacatConverters) {
        this.metacatHiveClient = metacatHiveClient;
        this.hiveMetacatConverters = hiveMetacatConverters;
        this.hiveConnectorDatabaseService = hiveConnectorDatabaseService;
        this.catalogName = catalogName;
    }

    /**
     * getTable.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to get
     * @return table dto
     */
    @Override
    public TableInfo get(@Nonnull final ConnectorContext requestContext, @Nonnull final QualifiedName name) {
        try {
            final Table table = metacatHiveClient.getTableByName(name.getDatabaseName(), name.getTableName());
            return hiveMetacatConverters.toTableInfo(name, table);
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(name, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed get hive table %s", name), exception);
        }
    }

    /**
     * Create a table.
     *
     * @param requestContext The request context
     * @param tableInfo      The resource metadata
     */
    @Override
    public void create(@Nonnull final ConnectorContext requestContext, @Nonnull final TableInfo tableInfo) {
        try {
            final Table table = hiveMetacatConverters.fromTableInfo(tableInfo);
            updateTable(requestContext, table, tableInfo);
            metacatHiveClient.createTable(table);

        } catch (MetaException exception) {
            throw new InvalidMetaException(tableInfo.getName(), exception);
        } catch (InvalidObjectException exception) {
            throw new DatabaseNotFoundException(
                    QualifiedName.ofDatabase(tableInfo.getName().getCatalogName(),
                            tableInfo.getName().getDatabaseName()), exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed create hive table %s", tableInfo.getName()), exception);
        }
    }

    private void updateTable(@Nonnull final ConnectorContext requestContext,
                             @Nonnull final Table table,
                             @Nonnull final TableInfo tableInfo) throws MetaException {
        if (table.getParameters() == null || table.getParameters().isEmpty()) {
            table.setParameters(Maps.newHashMap());
        }
        table.getParameters().putIfAbsent(PARAMETER_EXTERNAL, "TRUE");
        if (tableInfo.getMetadata() != null) {
            table.getParameters().putAll(tableInfo.getMetadata());
        }

        //storage
        final StorageDescriptor sd = table.getSd() != null ? table.getSd() : new StorageDescriptor();
        String inputFormat = null;
        String outputFormat = null;
        Map<String, String> sdParameters = Maps.newHashMap();
        final String location =
                tableInfo.getSerde() == null ? null : tableInfo.getSerde().getUri();
        if (location != null) {
            sd.setLocation(location);
        } else if (sd.getLocation() == null) {
            final String locationStr = hiveConnectorDatabaseService.get(requestContext,
                    QualifiedName.ofDatabase(tableInfo.getName().
                            getCatalogName(), tableInfo.getName().getDatabaseName())).getUri();
            final Path databasePath = new Path(locationStr);
            final Path targetPath = new Path(databasePath, tableInfo.getName().getTableName());
            sd.setLocation(targetPath.toString());
        }

        SerDeInfo serdeInfo = sd.getSerdeInfo();
        final StorageInfo storageInfo = tableInfo.getSerde();
        if (serdeInfo != null) {
            serdeInfo.setName(tableInfo.getName().getTableName());
            if (storageInfo != null) {
                if (!Strings.isNullOrEmpty(storageInfo.getSerializationLib())) {
                    serdeInfo.setSerializationLib(storageInfo.getSerializationLib());
                }
                if (storageInfo.getSerdeInfoParameters() != null && !storageInfo.getSerdeInfoParameters().isEmpty()) {
                    serdeInfo.setParameters(storageInfo.getSerdeInfoParameters());
                }
                inputFormat = storageInfo.getInputFormat();
                outputFormat = storageInfo.getOutputFormat();
                if (storageInfo.getParameters() != null && !storageInfo.getParameters().isEmpty()) {
                    sdParameters = storageInfo.getParameters();
                }
            }
        } else {
            serdeInfo = new SerDeInfo();
            serdeInfo.setName(tableInfo.getName().getTableName());
            if (storageInfo != null) {
                serdeInfo.setSerializationLib(storageInfo.getSerializationLib());
                if (storageInfo.getSerdeInfoParameters() != null && !storageInfo.getSerdeInfoParameters().isEmpty()) {
                    serdeInfo.setParameters(storageInfo.getSerdeInfoParameters());
                }
                inputFormat = storageInfo.getInputFormat();
                outputFormat = storageInfo.getOutputFormat();
                if (storageInfo.getParameters() != null && !storageInfo.getParameters().isEmpty()) {
                    sdParameters = storageInfo.getParameters();
                }
            } else {
                final HiveStorageFormat hiveStorageFormat = extractHiveStorageFormat(table);
                serdeInfo.setSerializationLib(hiveStorageFormat.getSerDe());
                serdeInfo.setParameters(ImmutableMap.<String, String>of());
                inputFormat = hiveStorageFormat.getInputFormat();
                outputFormat = hiveStorageFormat.getOutputFormat();
            }
            sd.setSerdeInfo(serdeInfo);
        }

        final ImmutableList.Builder<FieldSchema> columnsBuilder = ImmutableList.builder();
        final ImmutableList.Builder<FieldSchema> partitionKeysBuilder = ImmutableList.builder();
        if (tableInfo.getFields() != null) {
            for (FieldInfo column : tableInfo.getFields()) {
                final FieldSchema field = hiveMetacatConverters.metacatToHiveField(column);
                if (column.isPartitionKey()) {
                    partitionKeysBuilder.add(field);
                } else {
                    columnsBuilder.add(field);
                }
            }
        }
        final ImmutableList<FieldSchema> columns = columnsBuilder.build();
        if (!columns.isEmpty()) {
            sd.setCols(columns);
        }
        if (!Strings.isNullOrEmpty(inputFormat)) {
            sd.setInputFormat(inputFormat);
        }
        if (!Strings.isNullOrEmpty(outputFormat)) {
            sd.setOutputFormat(outputFormat);
        }
        if (sd.getParameters() == null) {
            sd.setParameters(sdParameters);
        }

        //partition keys
        final ImmutableList<FieldSchema> partitionKeys = partitionKeysBuilder.build();
        if (!partitionKeys.isEmpty()) {
            table.setPartitionKeys(partitionKeys);
        }
        table.setSd(sd);
    }

    private static HiveStorageFormat extractHiveStorageFormat(final Table table) throws MetaException {
        final StorageDescriptor descriptor = table.getSd();
        if (descriptor == null) {
            throw new MetaException("Table is missing storage descriptor");
        }
        final SerDeInfo serdeInfo = descriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new MetaException(
                    "Table storage descriptor is missing SerDe info");
        }
        final String outputFormat = descriptor.getOutputFormat();
        final String serializationLib = serdeInfo.getSerializationLib();

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerDe().equals(serializationLib)) {
                return format;
            }
        }
        throw new MetaException(
                String.format("Output format %s with SerDe %s is not supported", outputFormat, serializationLib));
    }

    /**
     * Delete a table with the given qualified name.
     *
     * @param requestContext The request context
     * @param name           The qualified name of the resource to delete
     */
    @Override
    public void delete(@Nonnull final ConnectorContext requestContext, @Nonnull final QualifiedName name) {
        try {
            metacatHiveClient.dropTable(name.getDatabaseName(), name.getTableName());
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(name, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed delete hive table %s", name), exception);
        }
    }

    /**
     * Update a resource with the given metadata.
     *
     * @param requestContext The request context
     * @param tableInfo      The resource metadata
     */
    @Override
    public void update(@Nonnull final ConnectorContext requestContext, @Nonnull final TableInfo tableInfo) {
        try {
            final Table existingTable = hiveMetacatConverters.fromTableInfo(get(requestContext, tableInfo.getName()));
            if (existingTable.getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
                throw new TableNotFoundException(tableInfo.getName());
            }
            updateTable(requestContext, existingTable, tableInfo);
            metacatHiveClient.alterTable(tableInfo.getName().getDatabaseName(),
                    tableInfo.getName().getTableName(),
                    existingTable);
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(tableInfo.getName(), exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(tableInfo.getName(), exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed update hive table %s", tableInfo.getName()), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<QualifiedName> listNames(
            @Nonnull final ConnectorContext requestContext,
            @Nonnull final QualifiedName name,
            @Nullable final QualifiedName prefix,
            @Nullable final Sort sort,
            @Nullable final Pageable pageable
    ) {
        try {
            List<QualifiedName> qualifiedNames = Lists.newArrayList();

            final String tableFilter = (prefix != null && prefix.isTableDefinition()) ? prefix.getTableName() : null;
            for (String tableName : metacatHiveClient.getAllTables(name.getDatabaseName())) {
                if (tableFilter == null || tableName.startsWith(tableFilter)) {
                    final QualifiedName qualifiedName =
                            QualifiedName.ofTable(name.getCatalogName(), name.getDatabaseName(), tableName);
                    if (prefix != null && !qualifiedName.toString().startsWith(prefix.toString())) {
                        continue;
                    }
                    qualifiedNames.add(qualifiedName);

                }
            }
            if (null != pageable && pageable.isPageable()) {
                final int limit = Math.min(pageable.getOffset() + pageable.getLimit(), qualifiedNames.size());
                qualifiedNames = (pageable.getOffset() > limit) ? Lists.newArrayList()
                        : qualifiedNames.subList(pageable.getOffset(), limit);
            }

            return qualifiedNames;
        } catch (MetaException exception) {
            throw new InvalidMetaException(name, exception);
        } catch (NoSuchObjectException exception) {
            throw new DatabaseNotFoundException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed listNames hive table %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<TableInfo> list(
            @Nonnull final ConnectorContext requestContext,
            @Nonnull final QualifiedName name,
            @Nullable final QualifiedName prefix,
            @Nullable final Sort sort,
            @Nullable final Pageable pageable
    ) {

        try {
            List<TableInfo> tableInfos = Lists.newArrayList();
            for (String tableName : metacatHiveClient.getAllTables(name.getDatabaseName())) {
                final QualifiedName qualifiedName = QualifiedName.ofDatabase(name.getCatalogName(), tableName);
                if (!qualifiedName.toString().startsWith(prefix.toString())) {
                    continue;
                }
                final Table table = metacatHiveClient.getTableByName(name.getDatabaseName(), tableName);
                tableInfos.add(hiveMetacatConverters.toTableInfo(name, table));
            }
            if (null != pageable && pageable.isPageable()) {
                final int limit = Math.min(pageable.getOffset() + pageable.getLimit(), tableInfos.size());
                tableInfos = (pageable.getOffset() > limit) ? Lists.newArrayList()
                        : tableInfos.subList(pageable.getOffset(), limit);
            }
            return tableInfos;
        } catch (MetaException exception) {
            throw new DatabaseNotFoundException(name, exception);
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed list hive table %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public boolean exists(@Nonnull final ConnectorContext requestContext, @Nonnull final QualifiedName name) {
        try {
            return metacatHiveClient.getTableByName(name.getDatabaseName(), name.getTableName()) != null;
        } catch (TException exception) {
            throw new ConnectorException(String.format("Failed exists hive table %s", name), exception);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void rename(
            @Nonnull final ConnectorContext context,
            @Nonnull final QualifiedName oldName,
            @Nonnull final QualifiedName newName
    ) {
        try {
            metacatHiveClient.rename(oldName.getDatabaseName(), oldName.getTableName(),
                    newName.getDatabaseName(), newName.getTableName());
        } catch (NoSuchObjectException exception) {
            throw new TableNotFoundException(oldName, exception);
        } catch (MetaException exception) {
            throw new InvalidMetaException(newName, exception);
        } catch (TException exception) {
            throw new ConnectorException(
                    "Failed renaming from hive table" + oldName.toString()
                            + " to hive talbe " + newName.toString(), exception);
        }
    }
}
