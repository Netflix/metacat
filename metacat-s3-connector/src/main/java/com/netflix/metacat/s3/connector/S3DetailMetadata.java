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

package com.netflix.metacat.s3.connector;

import com.facebook.presto.exception.SchemaAlreadyExistsException;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.HiveErrorCode;
import com.facebook.presto.hive.HiveOutputTableHandle;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveTableProperties;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.util.Types;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorDetailMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSchemaMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableDetailMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Preconditions;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.persist.Transactional;
import com.netflix.metacat.s3.connector.dao.DatabaseDao;
import com.netflix.metacat.s3.connector.dao.FieldDao;
import com.netflix.metacat.s3.connector.dao.SourceDao;
import com.netflix.metacat.s3.connector.dao.TableDao;
import com.netflix.metacat.s3.connector.model.Database;
import com.netflix.metacat.s3.connector.model.Field;
import com.netflix.metacat.s3.connector.model.Info;
import com.netflix.metacat.s3.connector.model.Location;
import com.netflix.metacat.s3.connector.model.Schema;
import com.netflix.metacat.s3.connector.model.Table;
import com.netflix.metacat.s3.connector.util.ConverterUtil;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * S3 detail metadata.
 */
@Transactional
public class S3DetailMetadata implements ConnectorDetailMetadata {
    @Inject
    private SourceDao sourceDao;
    @Inject
    private DatabaseDao databaseDao;
    @Inject
    private TableDao tableDao;
    @Inject
    private FieldDao fieldDao;
    @Inject
    private HiveConnectorId connectorId;
    @Inject
    private ConverterUtil converterUtil;
    @Inject
    private TypeManager typeManager;
    @Inject
    private HdfsEnvironment hdfsEnvironment;

    @Override
    public void createSchema(final ConnectorSession session, final ConnectorSchemaMetadata schema) {
        final String schemaName = schema.getSchemaName();
        Preconditions.checkNotNull(schemaName, "Schema name is null");
        if (databaseDao.getBySourceDatabaseName(connectorId.toString(), schemaName) != null) {
            throw new SchemaAlreadyExistsException(schemaName);
        }
        final Database database = new Database();
        database.setName(schemaName);
        database.setSource(sourceDao.getByName(connectorId.toString()));
        databaseDao.save(database);
    }

    @Override
    public void updateSchema(final ConnectorSession session, final ConnectorSchemaMetadata schema) {
        // no op
    }

    @Override
    public void dropSchema(final ConnectorSession session, final String schemaName) {
        Preconditions.checkNotNull(schemaName, "Schema name is null");
        final Database database = databaseDao.getByName(schemaName);
        if (database == null) {
            throw new SchemaNotFoundException(schemaName);
        } else if (database.getTables() != null && !database.getTables().isEmpty()) {
            throw new PrestoException(StandardErrorCode.CONSTRAINT_VIOLATION,
                "Schema " + schemaName + " is not empty. One or more tables exist.");
        }
        databaseDao.delete(database);
    }

    @Override
    public ConnectorSchemaMetadata getSchema(final ConnectorSession session, final String schemaName) {
        return new ConnectorSchemaMetadata(schemaName);
    }

    @Override
    public ConnectorTableHandle alterTable(final ConnectorSession session, final ConnectorTableMetadata tableMetadata) {
        final SchemaTableName tableName = tableMetadata.getTable();
        final Table table = tableDao
            .getBySourceDatabaseTableName(connectorId.toString(), tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(tableName);
        }
        //we can update the fields, the uri, or the full serde
        final Location newLocation = converterUtil.toLocation(tableMetadata);
        Location location = table.getLocation();
        if (location == null) {
            location = new Location();
            location.setTable(table);
            table.setLocation(location);
        }
        if (newLocation.getUri() != null) {
            location.setUri(newLocation.getUri());
        }
        final Info newInfo = newLocation.getInfo();
        if (newInfo != null) {
            final Info info = location.getInfo();
            if (info == null) {
                location.setInfo(newInfo);
                newInfo.setLocation(location);
            } else {
                if (newInfo.getInputFormat() != null) {
                    info.setInputFormat(newInfo.getInputFormat());
                }
                if (newInfo.getOutputFormat() != null) {
                    info.setOutputFormat(newInfo.getOutputFormat());
                }
                if (newInfo.getOwner() != null) {
                    info.setOwner(newInfo.getOwner());
                }
                if (newInfo.getSerializationLib() != null) {
                    info.setSerializationLib(newInfo.getSerializationLib());
                }
                if (newInfo.getParameters() != null && !newInfo.getParameters().isEmpty()) {
                    info.setParameters(newInfo.getParameters());
                }
            }
        }
        final Schema newSchema = newLocation.getSchema();
        if (newSchema != null) {
            final List<Field> newFields = newSchema.getFields();
            if (newFields != null && !newFields.isEmpty()) {
                final Schema schema = location.getSchema();
                if (schema == null) {
                    location.setSchema(newSchema);
                    newSchema.setLocation(location);
                } else {
                    final List<Field> fields = schema.getFields();
                    if (fields.isEmpty()) {
                        newFields.forEach(field -> {
                            field.setSchema(schema);
                            fields.add(field);
                        });
                    } else {
                        for (int i = 0; i < newFields.size(); i++) {
                            final Field newField = newFields.get(i);
                            newField.setPos(i);
                            newField.setSchema(schema);
                            if (newField.getType() == null) {
                                newField.setType(newField.getSourceType());
                            }
                        }
                        schema.setFields(null);
                        fieldDao.delete(fields);
                        tableDao.save(table, true);
                        schema.setFields(newFields);
                    }
                }
            }
        }
        return new HiveTableHandle(connectorId.toString(), tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public List<ConnectorTableMetadata> listTableMetadatas(final ConnectorSession session, final String schemaName,
        final List<String> tableNames) {
        final List<Table> tables =
            tableDao.getBySourceDatabaseTableNames(connectorId.toString(), schemaName, tableNames);
        return tables.stream()
            .map(table ->
                new ConnectorTableDetailMetadata(new SchemaTableName(schemaName, table.getName()),
                    converterUtil.toColumnMetadatas(table), converterUtil.getOwner(table),
                    converterUtil.toStorageInfo(table), null, converterUtil.toAuditInfo(table))
            )
            .collect(Collectors.toList());
    }

    @Override
    public List<String> listSchemaNames(final ConnectorSession session) {
        final List<Database> databases = sourceDao.getByName(connectorId.toString(), false).getDatabases();
        return databases.stream().map(database -> database.getName().toLowerCase(Locale.ENGLISH))
            .collect(Collectors.toList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(final ConnectorSession session, final SchemaTableName tableName) {
        return new HiveTableHandle(connectorId.toString(), tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(final ConnectorSession session,
        final ConnectorTableHandle tableHandle) {
        final SchemaTableName schemaTableName = HiveUtil.schemaTableName(tableHandle);
        return getTableMetadata(schemaTableName);
    }

    @Override
    public List<SchemaTableName> listTables(final ConnectorSession session, final String schemaNameOrNull) {
        final ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            final Database database = databaseDao.getBySourceDatabaseName(connectorId.toString(), schemaName);
            if (database != null) {
                for (Table table : database.getTables()) {
                    tableNames.add(new SchemaTableName(schemaName, table.getName().toLowerCase(Locale.ENGLISH)));
                }
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(final ConnectorSession session, final String schemaNameOrNull) {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(final ConnectorSession session,
        final ConnectorTableHandle tableHandle) {
        final SchemaTableName schemaTableName = HiveUtil.schemaTableName(tableHandle);
        final Table table = tableDao.getBySourceDatabaseTableName(connectorId.toString(),
            schemaTableName.getSchemaName(),
            schemaTableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        for (Field field : getFields(table)) {
            if (HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME.equals(field.getName())) {
                final String type = field.getType();
                final Type prestoType = converterUtil.toType(type);
                final HiveType hiveType = HiveType.toHiveType(prestoType);
                return new HiveColumnHandle(connectorId.toString(), field.getName(), field.getPos(),
                    hiveType, prestoType.getTypeSignature(), field.getPos(), field.isPartitionKey());
            }
        }
        return null;
    }

    @Override
    public boolean canCreateSampledTables(final ConnectorSession session) {
        return false;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(final ConnectorSession session,
        final ConnectorTableHandle tableHandle) {
        final SchemaTableName schemaTableName = HiveUtil.schemaTableName(tableHandle);
        final Table table = tableDao.getBySourceDatabaseTableName(connectorId.toString(),
            schemaTableName.getSchemaName(),
            schemaTableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        final ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (Field field : getFields(table)) {
            final String type = field.getType();
            final Type prestoType = converterUtil.toType(type);
            final HiveType hiveType = HiveType.toHiveType(prestoType);
            columnHandles
                .put(field.getName(), new HiveColumnHandle(connectorId.toString(), field.getName(), field.getPos(),
                    hiveType, prestoType.getTypeSignature(), field.getPos(), field.isPartitionKey()));
        }
        return columnHandles.build();
    }

    private List<Field> getFields(final Table table) {
        List<Field> result = Lists.newArrayList();
        final Location location = table.getLocation();
        if (location != null) {
            final Schema schema = location.getSchema();
            if (schema != null && schema.getFields() != null) {
                result = schema.getFields();
            }
        }
        return result;
    }

    @Override
    public ColumnMetadata getColumnMetadata(final ConnectorSession session, final ConnectorTableHandle tableHandle,
        final ColumnHandle columnHandle) {
        Types.checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        return Types.checkType(columnHandle, HiveColumnHandle.class, "columnHandle").getColumnMetadata(typeManager);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(final ConnectorSession session,
        final SchemaTablePrefix prefix) {
        Preconditions.checkNotNull(prefix, "prefix is null");
        final ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            } catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(final SchemaTableName schemaTableName) {
        final Table table = tableDao.getBySourceDatabaseTableName(connectorId.toString(),
            schemaTableName.getSchemaName(),
            schemaTableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return new ConnectorTableDetailMetadata(new SchemaTableName(schemaTableName.getSchemaName(), table.getName()),
            converterUtil.toColumnMetadatas(table), converterUtil.getOwner(table),
            converterUtil.toStorageInfo(table), null, converterUtil.toAuditInfo(table));
    }

    private List<SchemaTableName> listTables(final ConnectorSession session, final SchemaTablePrefix prefix) {
        if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public void createTable(final ConnectorSession session, final ConnectorTableMetadata tableMetadata) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        final SchemaTableName schemaTableName = tableMetadata.getTable();
        if (tableDao.getBySourceDatabaseTableName(connectorId.toString(), schemaTableName.getSchemaName(),
            schemaTableName.getTableName()) != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }
        final Database database = databaseDao
            .getBySourceDatabaseName(connectorId.toString(), schemaTableName.getSchemaName());
        if (database == null) {
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }
        final Table table = new Table();
        table.setName(schemaTableName.getTableName());
        table.setDatabase(database);
        final Location location = converterUtil.toLocation(tableMetadata);
        if (location != null) {
            location.setTable(table);
            table.setLocation(location);
        }
        tableDao.save(table);
    }

    @Override
    public void dropTable(final ConnectorSession session, final ConnectorTableHandle tableHandle) {
        final HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        final Table table = tableDao.getBySourceDatabaseTableName(connectorId.toString(),
            hiveTableHandle.getSchemaName(),
            hiveTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(
                new SchemaTableName(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName()));
        }
        tableDao.delete(table);
    }

    @Override
    public void renameTable(final ConnectorSession session, final ConnectorTableHandle tableHandle,
        final SchemaTableName newTableName) {
        final HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        final Table table = tableDao.getBySourceDatabaseTableName(connectorId.toString(),
            hiveTableHandle.getSchemaName(),
            hiveTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(
                new SchemaTableName(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName()));
        }
        final Table newTable = tableDao.getBySourceDatabaseTableName(connectorId.toString(),
            newTableName.getSchemaName(),
            newTableName.getTableName());
        if (newTable == null) {
            table.setName(newTableName.getTableName());
            tableDao.save(table);
        } else {
            throw new TableAlreadyExistsException(newTableName, "Table with new name already exists");
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(final ConnectorSession session,
        final ConnectorTableMetadata tableMetadata) {

        Preconditions.checkArgument(!Strings.isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        final HiveStorageFormat hiveStorageFormat =
            HiveTableProperties.getHiveStorageFormat(tableMetadata.getProperties());
        final ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        final ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();

        // get the root directory for the database
        final SchemaTableName schemaTableName = tableMetadata.getTable();
        final String schemaName = schemaTableName.getSchemaName();
        final String tableName = schemaTableName.getTableName();

        buildColumnInfo(tableMetadata, columnNames, columnTypes);

        final Path targetPath = getTargetPath(schemaName, tableName, schemaTableName);

        // use a per-user temporary directory to avoid permission problems
        // TODO: this should use Hadoop UserGroupInformation
        final String temporaryPrefix = "/tmp/presto-" + StandardSystemProperty.USER_NAME.value();

        // create a temporary directory on the same filesystem
        final Path temporaryRoot = new Path(targetPath, temporaryPrefix);
        final Path temporaryPath = new Path(temporaryRoot, UUID.randomUUID().toString());
        createDirectories(temporaryPath);

        return new HiveOutputTableHandle(
            connectorId.toString(),
            schemaName,
            tableName,
            columnNames.build(),
            columnTypes.build(),
            tableMetadata.getOwner(),
            targetPath.toString(),
            temporaryPath.toString(),
            hiveStorageFormat);
    }

    private Path getTargetPath(final String schemaName, final String tableName, final SchemaTableName schemaTableName) {
        final String location = sourceDao.getByName(connectorId.toString()).getThriftUri();
        if (Strings.isNullOrEmpty(location)) {
            throw new PrestoException(HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR,
                String.format("Database '%s' location is not set", schemaName));
        }

        final Path databasePath = new Path(location);
        if (!pathExists(databasePath)) {
            throw new PrestoException(HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR,
                String.format("Database '%s' location does not exist: %s", schemaName, databasePath));
        }
        if (!isDirectory(databasePath)) {
            throw new PrestoException(HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR,
                String.format("Database '%s' location is not a directory: %s", schemaName, databasePath));
        }

        // verify the target directory for the table
        final Path targetPath = new Path(databasePath, tableName);
        if (pathExists(targetPath)) {
            throw new PrestoException(HiveErrorCode.HIVE_PATH_ALREADY_EXISTS,
                String.format("Target directory for table '%s' already exists: %s", schemaTableName, targetPath));
        }
        return targetPath;
    }

    private boolean pathExists(final Path path) {
        try {
            return hdfsEnvironment.getFileSystem(path).exists(path);
        } catch (IOException e) {
            throw new PrestoException(HiveErrorCode.HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    private boolean isDirectory(final Path path) {
        try {
            return hdfsEnvironment.getFileSystem(path).isDirectory(path);
        } catch (IOException e) {
            throw new PrestoException(HiveErrorCode.HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    private void createDirectories(final Path path) {
        try {
            if (!hdfsEnvironment.getFileSystem(path).mkdirs(path)) {
                throw new IOException("mkdirs returned false");
            }
        } catch (IOException e) {
            throw new PrestoException(HiveErrorCode.HIVE_FILESYSTEM_ERROR, "Failed to create directory: " + path, e);
        }
    }

    private static void buildColumnInfo(final ConnectorTableMetadata tableMetadata,
        final ImmutableList.Builder<String> names,
        final ImmutableList.Builder<Type> types) {
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            names.add(column.getName());
            types.add(column.getType());
        }

        if (tableMetadata.isSampled()) {
            names.add(HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME);
            types.add(BigintType.BIGINT);
        }
    }

    @Override
    public void commitCreateTable(final ConnectorSession session, final ConnectorOutputTableHandle tableHandle,
        final Collection<Slice> fragments) {
        final HiveOutputTableHandle handle = Types.checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");

        // verify no one raced us to create the target directory
        final Path targetPath = new Path(handle.getTargetPath());

        // rename if using a temporary directory
        if (handle.hasTemporaryPath()) {
            if (pathExists(targetPath)) {
                final SchemaTableName table = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
                throw new PrestoException(HiveErrorCode.HIVE_PATH_ALREADY_EXISTS,
                    String.format("Unable to commit creation of table '%s': target directory already exists: %s", table,
                        targetPath));
            }
            // rename the temporary directory to the target
            rename(new Path(handle.getTemporaryPath()), targetPath);
        }

        // create the table in the metastore
        final List<String> types = handle.getColumnTypes().stream()
            .map(HiveType::toHiveType)
            .map(HiveType::getHiveTypeName)
            .collect(Collectors.toList());

        boolean sampled = false;
        final ImmutableList.Builder<Field> columns = ImmutableList.builder();
        for (int i = 0; i < handle.getColumnNames().size(); i++) {
            final String name = handle.getColumnNames().get(i);
            final String type = types.get(i);
            final Field field = new Field();
            field.setName(name);
            field.setPos(i);
            field.setType(type);
            if (name.equals(HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME)) {
                field.setComment("Presto sample weight column");
                sampled = true;
            }
            columns.add(field);
        }

        final HiveStorageFormat hiveStorageFormat = handle.getHiveStorageFormat();

        final Database database = databaseDao.getBySourceDatabaseName(connectorId.toString(), handle.getSchemaName());

        final Table table = new Table();
        table.setName(handle.getTableName());
        table.setDatabase(database);

        final Location location = new Location();
        location.setTable(table);
        table.setLocation(location);
        location.setUri(targetPath.toString());
        final Info info = new Info();
        info.setLocation(location);
        info.setInputFormat(hiveStorageFormat.getInputFormat());
        info.setOutputFormat(hiveStorageFormat.getOutputFormat());
        info.setOwner(handle.getTableOwner());
        info.setParameters(ImmutableMap.<String, String>of());
        info.setSerializationLib(hiveStorageFormat.getSerDe());
        location.setInfo(info);

        final Schema schema = new Schema();
        schema.setLocation(location);
        schema.setFields(columns.build());
        location.setSchema(schema);

        tableDao.save(table);
    }

    private void rename(final Path source, final Path target) {
        try {
            if (!hdfsEnvironment.getFileSystem(source).rename(source, target)) {
                throw new IOException("rename returned false");
            }
        } catch (IOException e) {
            throw new PrestoException(HiveErrorCode.HIVE_FILESYSTEM_ERROR,
                String.format("Failed to rename %s to %s", source, target), e);
        }
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(final ConnectorSession session,
        final ConnectorTableHandle tableHandle) {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "INSERT not yet supported for S3");
    }

    @Override
    public void commitInsert(final ConnectorSession session, final ConnectorInsertTableHandle insertHandle,
        final Collection<Slice> fragments) {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "INSERT not yet supported for S3");
    }

    @Override
    public void createView(final ConnectorSession session, final SchemaTableName viewName,
        final String viewData, final boolean replace) {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Views not yet supported for S3");
    }

    @Override
    public void dropView(final ConnectorSession session, final SchemaTableName viewName) {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Views not yet supported for S3");
    }

    @Override
    public List<SchemaTableName> listViews(final ConnectorSession session, final String schemaNameOrNull) {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Views not yet supported for S3");
    }

    @Override
    public Map<SchemaTableName, String> getViews(final ConnectorSession session, final SchemaTablePrefix prefix) {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Views not yet supported for S3");
    }
}
