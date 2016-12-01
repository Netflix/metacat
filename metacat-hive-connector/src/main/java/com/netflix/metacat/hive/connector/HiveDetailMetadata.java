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

package com.netflix.metacat.hive.connector;

import com.facebook.presto.exception.SchemaAlreadyExistsException;
import com.facebook.presto.hive.ForHiveClient;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.HiveErrorCode;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.hive.util.Types;
import com.facebook.presto.spi.ColumnDetailMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorDetailMetadata;
import com.facebook.presto.spi.ConnectorSchemaMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableDetailMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.StorageInfo;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.converters.impl.HiveTypeConverter;
import com.netflix.metacat.hive.connector.util.ConverterUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Hive connector detail metadata.
 */
public class HiveDetailMetadata extends HiveMetadata implements ConnectorDetailMetadata {
    /** Name for external parameter. */
    public static final String PARAMETER_EXTERNAL = "EXTERNAL";
    protected final HiveMetastore metastore;
    protected final TypeManager typeManager;
    protected final HiveConnectorId connectorId;
    protected ConverterUtil converterUtil;
    protected HiveTypeConverter hiveTypeConverter;

    /**
     * Constructor.
     * @param connectorId connector id
     * @param hiveClientConfig config
     * @param metastore metastore
     * @param hdfsEnvironment hdfs
     * @param partitionManager manager
     * @param executorService executor
     * @param typeManager type manager
     * @param converterUtil converter
     * @param hiveTypeConverter type converter
     */
    @Inject
    public HiveDetailMetadata(final HiveConnectorId connectorId,
        final HiveClientConfig hiveClientConfig,
        final HiveMetastore metastore,
        final HdfsEnvironment hdfsEnvironment,
        final HivePartitionManager partitionManager,
        @ForHiveClient
        final ExecutorService executorService, final TypeManager typeManager,
        final ConverterUtil converterUtil, final HiveTypeConverter hiveTypeConverter) {
        super(connectorId, hiveClientConfig, metastore, hdfsEnvironment, partitionManager, executorService,
            typeManager);
        this.metastore = metastore;
        this.typeManager = typeManager;
        this.connectorId = connectorId;
        this.converterUtil = converterUtil;
        this.hiveTypeConverter = hiveTypeConverter;
    }

    @Override
    public void createSchema(final ConnectorSession session, final ConnectorSchemaMetadata schema) {
        Preconditions.checkNotNull(schema.getSchemaName(), "Schema name is null");
        try {
            final Database database = new Database(schema.getSchemaName(), null, schema.getUri(), schema.getMetadata());
            ((MetacatHiveMetastore) metastore).createDatabase(database);
            // If a method is ever exposed to flush only database related caches that could replace flushing everything
            metastore.flushCache();
        } catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(schema.getSchemaName());
        }
    }

    @Override
    public void updateSchema(final ConnectorSession session, final ConnectorSchemaMetadata schema) {
        Preconditions.checkNotNull(schema.getSchemaName(), "Schema name is null");
        try {
            final Database database = new Database(schema.getSchemaName(), null, schema.getUri(), schema.getMetadata());
            ((MetacatHiveMetastore) metastore).updateDatabase(database);
        } catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(schema.getSchemaName());
        }
    }

    @Override
    public void dropSchema(final ConnectorSession session, final String schemaName) {
        Preconditions.checkNotNull(schemaName, "Schema name is null");
        try {
            ((MetacatHiveMetastore) metastore).dropDatabase(schemaName);
            metastore.flushCache();
        } catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    @Override
    public ConnectorSchemaMetadata getSchema(final ConnectorSession session, final String schemaName) {
        Preconditions.checkNotNull(schemaName, "Schema name is null");
        final Database database = metastore.getDatabase(schemaName)
            .orElseThrow(() -> new SchemaNotFoundException(schemaName));
        return new ConnectorSchemaMetadata(schemaName, database.getLocationUri(), database.getParameters());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(final ConnectorSession session,
        final ConnectorTableHandle tableHandle) {
        Preconditions.checkNotNull(tableHandle, "tableHandle is null");
        final SchemaTableName tableName = HiveUtil.schemaTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableDetailMetadata getTableMetadata(final SchemaTableName tableName) {
        final Optional<Table> oTable = getMetastore().getTable(tableName.getSchemaName(), tableName.getTableName());
        final Table table = oTable.orElseThrow(() -> new TableNotFoundException(tableName));
        List<ColumnMetadata> columns = null;
        try {
            if (table.getSd().getColsSize() == 0) {
                final List<MetacatHiveColumnHandle> handles = hiveColumnHandles(typeManager, connectorId.toString(),
                    table,
                    false);
                columns = ImmutableList
                    .copyOf(Iterables.transform(handles, columnMetadataGetter(table, typeManager)));
            }
        } catch (Exception ignored) {
            // Ignore the error. It could be that the table is corrupt.
        }

        if (columns == null) {
            if (table.getSd().getColsSize() != 0) {
                columns = converterUtil.toColumnMetadatas(table, typeManager);
            } else {
                columns = Lists.newArrayList();
            }
        }

        return new ConnectorTableDetailMetadata(tableName, columns, table.getOwner(),
            converterUtil.toStorageInfo(table.getSd()), table.getParameters(),
            converterUtil.toAuditInfo(table));
    }

    /**
     * Hive columns.
     * @param pTypeManager type manager
     * @param pConnectorId connector id
     * @param table table
     * @param includeSampleWeight weight
     * @return list of columns
     */
    public List<MetacatHiveColumnHandle> hiveColumnHandles(final TypeManager pTypeManager, final String pConnectorId,
        final Table table,
        final boolean includeSampleWeight) {
        final ImmutableList.Builder<MetacatHiveColumnHandle> columns = ImmutableList.builder();

        // add the data fields first
        int hiveColumnIndex = 0;
        for (StructField field : HiveUtil.getTableStructFields(table)) {
            if ((includeSampleWeight || !field.getFieldName().equals(HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME))) {
                final Type type = hiveTypeConverter.getType(field.getFieldObjectInspector(), pTypeManager);
                HiveUtil.checkCondition(type != null, StandardErrorCode.NOT_SUPPORTED, "Unsupported Hive type: %s",
                    field.getFieldObjectInspector().getTypeName());
                columns.add(new MetacatHiveColumnHandle(pConnectorId, field.getFieldName(), hiveColumnIndex,
                    field.getFieldObjectInspector().getTypeName(), type, hiveColumnIndex, false));
            }
            hiveColumnIndex++;
        }

        // add the partition keys last (like Hive does)
        columns.addAll(getPartitionKeyColumnHandles(pConnectorId, table, hiveColumnIndex));

        return columns.build();
    }

    /**
     * Partition key columns.
     * @param pConnectorId connector id
     * @param table table
     * @param startOrdinal index
     * @return columns
     */
    public List<MetacatHiveColumnHandle> getPartitionKeyColumnHandles(final String pConnectorId, final Table table,
        final int startOrdinal) {
        final ImmutableList.Builder<MetacatHiveColumnHandle> columns = ImmutableList.builder();

        final List<FieldSchema> partitionKeys = table.getPartitionKeys();
        for (int i = 0; i < partitionKeys.size(); i++) {
            final FieldSchema field = partitionKeys.get(i);

            columns.add(new MetacatHiveColumnHandle(pConnectorId, field.getName(), startOrdinal + i, field.getType(),
                hiveTypeConverter.toType(
                    field.getType(), typeManager), -1, true));
        }

        return columns.build();
    }

    static Function<MetacatHiveColumnHandle, ColumnMetadata> columnMetadataGetter(final Table table,
        final TypeManager typeManager) {
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (FieldSchema field : Iterables.concat(table.getSd().getCols(), table.getPartitionKeys())) {
            if (field.getComment() != null) {
                builder.put(field.getName(), field.getComment());
            }
        }
        final Map<String, String> columnComment = builder.build();

        return input -> new ColumnDetailMetadata(
            input.getName(),
            input.getType(),
            input.isPartitionKey(),
            columnComment.get(input.getName()),
            false,
            input.getHiveTypeName()
        );
    }

    @Override
    public void createTable(final ConnectorSession session, final ConnectorTableMetadata tableMetadata) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        final Table table = new Table();
        table.setDbName(tableMetadata.getTable().getSchemaName());
        table.setTableName(tableMetadata.getTable().getTableName());
        table.setOwner(tableMetadata.getOwner());
        table.setTableType(TableType.EXTERNAL_TABLE.toString());
        if (tableMetadata instanceof ConnectorTableDetailMetadata) {
            updateTable(table, session, (ConnectorTableDetailMetadata) tableMetadata);
        }

        metastore.createTable(table);
    }

    private void updateTable(final Table table, final ConnectorSession session,
        final ConnectorTableDetailMetadata tableDetailMetadata) {
        if (table.getParameters() == null) {
            table.setParameters(Maps.newHashMap());
        }
        table.getParameters().putIfAbsent(PARAMETER_EXTERNAL, "TRUE");
        if (tableDetailMetadata.getMetadata() != null) {
            table.getParameters().putAll(tableDetailMetadata.getMetadata());
        }

        //storage
        final StorageDescriptor sd = table.getSd() != null ? table.getSd() : new StorageDescriptor();
        String inputFormat = null;
        String outputFormat = null;
        Map<String, String> sdParameters = Maps.newHashMap();
        final String location =
            tableDetailMetadata.getStorageInfo() == null ? null : tableDetailMetadata.getStorageInfo().getUri();
        if (location != null) {
            sd.setLocation(location);
        } else if (sd.getLocation() == null) {
            final String locationStr = getDatabase(tableDetailMetadata.getTable().getSchemaName()).getLocationUri();
            final Path databasePath = new Path(locationStr);
            final Path targetPath = new Path(databasePath, tableDetailMetadata.getTable().getTableName());
            sd.setLocation(targetPath.toString());
        }

        SerDeInfo serdeInfo = sd.getSerdeInfo();
        final StorageInfo storageInfo = tableDetailMetadata.getStorageInfo();
        if (serdeInfo != null) {
            serdeInfo.setName(tableDetailMetadata.getTable().getTableName());
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
            serdeInfo.setName(tableDetailMetadata.getTable().getTableName());
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
        for (ColumnMetadata column : tableDetailMetadata.getColumns()) {
            final FieldSchema field = converterUtil.toFieldSchema(column);
            if (column.isPartitionKey()) {
                partitionKeysBuilder.add(field);
            } else {
                columnsBuilder.add(field);
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

    private static HiveStorageFormat extractHiveStorageFormat(final Table table) {
        final StorageDescriptor descriptor = table.getSd();
        if (descriptor == null) {
            throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }
        final SerDeInfo serdeInfo = descriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA,
                "Table storage descriptor is missing SerDe info");
        }
        final String outputFormat = descriptor.getOutputFormat();
        final String serializationLib = serdeInfo.getSerializationLib();

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerDe().equals(serializationLib)) {
                return format;
            }
        }
        throw new PrestoException(HiveErrorCode.HIVE_UNSUPPORTED_FORMAT,
            String.format("Output format %s with SerDe %s is not supported", outputFormat, serializationLib));
    }

    @Override
    public ConnectorTableHandle alterTable(final ConnectorSession session, final ConnectorTableMetadata tableMetadata) {
        final Optional<Table> oTable = metastore
            .getTable(tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName());
        final Table table = oTable.orElseThrow(() -> new TableNotFoundException(tableMetadata.getTable()));
        try {
            if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
                throw new TableNotFoundException(tableMetadata.getTable());
            }
            if (tableMetadata instanceof ConnectorTableDetailMetadata) {
                updateTable(table, session, (ConnectorTableDetailMetadata) tableMetadata);
            }
            ((MetacatHiveMetastore) metastore).alterTable(table);
            return new HiveTableHandle(connectorId.toString(), tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName());
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableMetadata.getTable());
        }
    }

    /**
     * Deletes the table from hive. If the table is corrupted, it will still delete it instead of throwing an error.
     * @param tableHandle table
     */
    @Override
    public void dropTable(final ConnectorSession session, final ConnectorTableHandle tableHandle) {
        final HiveTableHandle handle = Types.checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        metastore.dropTable(handle.getSchemaName(), handle.getTableName());
        // To clear the database cache
        metastore.flushCache();
    }

    /**
     * Similar to listTables but this method will return the list of tables along with its metadata.
     * @param session connector session
     * @param schemaName schema name
     * @return list of table metadata.
     */
    @Override
    public List<ConnectorTableMetadata> listTableMetadatas(final ConnectorSession session, final String schemaName,
        final List<String> tableNames) {
        final List<Table> tables = ((MetacatHiveMetastore) metastore).getTablesByNames(schemaName, tableNames);
        if (tables != null) {
            return tables.stream().map(table -> {
                final List<ColumnMetadata> columns;
                if (table.getSd().getColsSize() == 0) {
                    final List<MetacatHiveColumnHandle> handles = hiveColumnHandles(typeManager, connectorId.toString(),
                        table, false);
                    columns = ImmutableList
                        .copyOf(Iterables.transform(handles, columnMetadataGetter(table, typeManager)));
                } else {
                    columns = converterUtil.toColumnMetadatas(table, typeManager);
                }
                final SchemaTableName tableName = new SchemaTableName(schemaName, table.getTableName());
                return new ConnectorTableDetailMetadata(tableName, columns, table.getOwner(),
                    converterUtil.toStorageInfo(table.getSd()), Maps.newHashMap(), converterUtil.toAuditInfo(table));
            }).collect(Collectors.toList());
        }
        return Lists.newArrayList();
    }

    private Database getDatabase(final String database) {
        return metastore.getDatabase(database).orElseThrow(() -> new SchemaNotFoundException(database));
    }
}
