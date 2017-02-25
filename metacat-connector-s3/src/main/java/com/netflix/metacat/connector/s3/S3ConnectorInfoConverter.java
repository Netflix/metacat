/*
 *
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
 *
 */
package com.netflix.metacat.connector.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeManager;
import com.netflix.metacat.common.type.TypeSignature;
import com.netflix.metacat.connector.pig.converters.PigTypeConverter;
import com.netflix.metacat.connector.s3.model.Database;
import com.netflix.metacat.connector.s3.model.Field;
import com.netflix.metacat.connector.s3.model.Info;
import com.netflix.metacat.connector.s3.model.Location;
import com.netflix.metacat.connector.s3.model.Partition;
import com.netflix.metacat.connector.s3.model.Schema;
import com.netflix.metacat.connector.s3.model.Source;
import com.netflix.metacat.connector.s3.model.Table;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts S3 model objects to Metacat DTOs and vice versa.
 * @author amajumdar
 */
public class S3ConnectorInfoConverter implements ConnectorInfoConverter<Database, Table, Partition> {
    private final PigTypeConverter pigTypeConverter;
    private final boolean isUsePigTypes;
    private final TypeManager typeManager;

    /**
     * Constructor.
     * @param pigTypeConverter Type converter for PIG
     * @param isUsePigTypes true, if we need to use pig type converter
     * @param typeManager Type manager
     */
    public S3ConnectorInfoConverter(final PigTypeConverter pigTypeConverter, final boolean isUsePigTypes,
        final TypeManager typeManager) {
        this.pigTypeConverter = pigTypeConverter;
        this.isUsePigTypes = isUsePigTypes;
        this.typeManager = typeManager;
    }

    @Override
    public DatabaseInfo toDatabaseInfo(final QualifiedName catalogName, final Database database) {
        final AuditInfo auditInfo = AuditInfo.builder().createdDate(database.getCreatedDate())
            .lastModifiedDate(database.getLastUpdatedDate()).build();
        return DatabaseInfo.builder().name(QualifiedName.ofDatabase(catalogName.getCatalogName(), database.getName()))
            .auditInfo(auditInfo).build();
    }

    @Override
    public Database fromDatabaseInfo(final DatabaseInfo databaseInfo) {
        final Database result = new Database();
        final QualifiedName databaseName = databaseInfo.getName();
        result.setName(databaseName.getDatabaseName());
        final Source source = new Source();
        source.setName(databaseName.getCatalogName());
        result.setSource(source);
        return result;
    }

    @Override
    public TableInfo toTableInfo(final QualifiedName tableName, final Table table) {
        return TableInfo.builder().name(tableName).fields(toFields(table)).auditInfo(toAuditInfo(table))
            .serde(toStorageInfo(table)).build();
    }

    private List<FieldInfo> toFields(final Table table) {
        List<FieldInfo> result = Lists.newArrayList();
        final Location location = table.getLocation();
        if (location != null) {
            final Schema schema = location.getSchema();
            if (schema != null) {
                result = schema.getFields().stream().sorted(Comparator.comparing(Field::getPos))
                    .map(this::toFieldInfo).collect(Collectors.toList());
            }
        }
        return result;
    }

    @Override
    public Table fromTableInfo(final TableInfo tableInfo) {
        final Table result = new Table();
        result.setName(tableInfo.getName().getTableName());
        final Location location = toLocation(tableInfo);
        if (location != null) {
            result.setLocation(location);
            location.setTable(result);
        }
        return result;
    }

    /**
     * Creates the s3 table.
     * @param database s3 database
     * @param tableInfo table info
     * @return s3 table
     */
    public Table fromTableInfo(final Database database, final TableInfo tableInfo) {
        final Table result = fromTableInfo(tableInfo);
        result.setDatabase(database);
        return result;
    }

    @Override
    public PartitionInfo toPartitionInfo(final TableInfo tableInfo, final Partition partition) {
        final QualifiedName tableName = tableInfo.getName();
        final StorageInfo storageInfo = tableInfo.getSerde();
        storageInfo.setUri(partition.getUri());
        final AuditInfo auditInfo = AuditInfo.builder().createdDate(partition.getCreatedDate())
            .lastModifiedDate(partition.getLastUpdatedDate())
            .build();
        final AuditInfo tableAuditInfo = tableInfo.getAudit();
        if (tableAuditInfo != null) {
            auditInfo.setCreatedBy(tableAuditInfo.getCreatedBy());
            auditInfo.setLastModifiedBy(tableAuditInfo.getLastModifiedBy());
        }
        return PartitionInfo.builder()
            .name(QualifiedName.ofPartition(tableName.getCatalogName(),
                tableName.getDatabaseName(), tableName.getTableName(), partition.getName()))
            .serde(storageInfo)
            .auditInfo(auditInfo)
            .build();
    }

    @Override
    public Partition fromPartitionInfo(final TableInfo tableInfo, final PartitionInfo partitionInfo) {
        return fromPartitionInfo(partitionInfo);
    }

    /**
     * Converts from partition info to s3 partition object.
     * @param partitionInfo partition info
     * @return s3 partition
     */
    Partition fromPartitionInfo(final PartitionInfo partitionInfo) {
        final Partition result = new Partition();
        result.setName(partitionInfo.getName().getPartitionName());
        result.setUri(partitionInfo.getSerde().getUri());
        final AuditInfo auditInfo = partitionInfo.getAudit();
        if (auditInfo != null) {
            result.setCreatedDate(auditInfo.getCreatedDate());
            result.setLastUpdatedDate(auditInfo.getLastModifiedDate());
        }
        return result;
    }

    /**
     * Returns a partition info.
     * @param tableName table name
     * @param table s3 table
     * @param partition partition
     * @return partition info
     */
    PartitionInfo toPartitionInfo(final QualifiedName tableName, final Table table, final Partition partition) {
        final StorageInfo storageInfo = toStorageInfo(table);
        storageInfo.setUri(partition.getUri());
        final AuditInfo auditInfo = AuditInfo.builder().createdDate(partition.getCreatedDate())
            .lastModifiedDate(partition.getLastUpdatedDate())
            .build();
        final AuditInfo tableAuditInfo = toAuditInfo(table);
        if (tableAuditInfo != null) {
            auditInfo.setCreatedBy(tableAuditInfo.getCreatedBy());
            auditInfo.setLastModifiedBy(tableAuditInfo.getLastModifiedBy());
        }
        return PartitionInfo.builder()
            .name(QualifiedName.ofPartition(tableName.getCatalogName(),
                tableName.getDatabaseName(), tableName.getTableName(), partition.getName()))
            .serde(storageInfo)
            .auditInfo(auditInfo)
            .build();
    }

    /**
     * Converts from s3 table info to storage info.
     * @param table table info
     * @return table info
     */
    StorageInfo toStorageInfo(final Table table) {
        StorageInfo result = null;
        final Location location = table.getLocation();
        if (location != null) {
            result = new StorageInfo();
            result.setUri(location.getUri());
            final Info info = location.getInfo();
            if (info != null) {
                result.setOwner(info.getOwner());
                result.setInputFormat(info.getInputFormat());
                result.setOutputFormat(info.getOutputFormat());
                result.setSerializationLib(info.getSerializationLib());
                final Map<String, String> infoParameters = Maps.newHashMap();
                if (info.getParameters() != null) {
                    infoParameters.putAll(info.getParameters());
                }
                result.setSerdeInfoParameters(infoParameters);
            }
        }
        return result;
    }

    /**
     * Gets the owner for the given table.
     * @param table table info
     * @return owner name
     */
    public String getOwner(final Table table) {
        String result = null;
        final Location location = table.getLocation();
        if (location != null) {
            final Info info = location.getInfo();
            if (info != null) {
                result = info.getOwner();
            }
        }
        return result;
    }

    /**
     * Converts from storage info to s3 location.
     * @param storageInfo storage info
     * @return location
     */
    Location fromStorageInfo(final StorageInfo storageInfo) {
        final Location result = new Location();
        if (storageInfo != null) {
            result.setUri(storageInfo.getUri());
            final Info info = new Info();
            info.setLocation(result);
            info.setOwner(storageInfo.getOwner());
            info.setInputFormat(storageInfo.getInputFormat());
            info.setOutputFormat(storageInfo.getOutputFormat());
            info.setSerializationLib(storageInfo.getSerializationLib());
            final Map<String, String> parameters = Maps.newHashMap();
            if (storageInfo.getParameters() != null) {
                parameters.putAll(storageInfo.getParameters());
            }
            if (storageInfo.getSerdeInfoParameters() != null) {
                parameters.putAll(storageInfo.getSerdeInfoParameters());
            }
            info.setParameters(parameters);
            result.setInfo(info);
        }
        return result;
    }

    /**
     * Creates list of fields from table info.
     * @param tableInfo table info
     * @param schema schema
     * @return list of fields
     */
    public List<Field> toFields(final TableInfo tableInfo, final Schema schema) {
        final ImmutableList.Builder<Field> columns = ImmutableList.builder();
        int index = 0;
        for (FieldInfo fieldInfo : tableInfo.getFields()) {
            final Field field = toField(fieldInfo);
            field.setPos(index++);
            field.setSchema(schema);
            columns.add(field);
        }
        return columns.build();
    }

    /**
     * Converts from column metadata to field.
     * @param fieldInfo column
     * @return field
     */
    public Field toField(final FieldInfo fieldInfo) {
        final Field result = new Field();
        result.setName(fieldInfo.getName());
        result.setPartitionKey(fieldInfo.isPartitionKey());
        result.setComment(fieldInfo.getComment());
        result.setSourceType(fieldInfo.getSourceType());
        result.setType(toTypeString(fieldInfo.getType()));
        return result;
    }

    /**
     * Converts from column metadata to field.
     * @param field column
     * @return field info
     */
    public FieldInfo toFieldInfo(final Field field) {
        return FieldInfo.builder().name(field.getName()).partitionKey(field.isPartitionKey())
            .comment(field.getComment()).sourceType(field.getSourceType()).type(toType(field.getType())).build();
    }

    private String toTypeString(final Type type) {
        String result = null;
        if (isUsePigTypes) {
            result = pigTypeConverter.fromMetacatType(type);
        } else {
            result = type.getDisplayName();
        }
        return result;
    }

    /**
     * Converts from type string to presto type.
     * @param type type
     * @return Type
     */
    public Type toType(final String type) {
        Type result = null;
        if (isUsePigTypes) {
            //Hack for now. We need to correct the type format in Franklin
            String typeString = type;
            if ("map".equals(type)) {
                typeString = "map[]";
            }
            result = pigTypeConverter.toMetacatType(typeString);
        } else {
            result = typeManager.getType(TypeSignature.parseTypeSignature(type));
        }
        return result;
    }

    /**
     * Creates audit info from s3 table info.
     * @param table table info
     * @return audit info
     */
    public AuditInfo toAuditInfo(final Table table) {
        final AuditInfo result = AuditInfo.builder().createdDate(table.getCreatedDate())
            .lastModifiedDate(table.getLastUpdatedDate()).build();
        final Location location = table.getLocation();
        if (location != null) {
            final Info info = location.getInfo();
            if (info != null) {
                result.setCreatedBy(info.getOwner());
                result.setLastModifiedBy(info.getOwner());
            }
        }
        return result;
    }

    /**
     * Creates location.
     * @param tableInfo table info
     * @return location
     */
    public Location toLocation(final TableInfo tableInfo) {
        final Location location = fromStorageInfo(tableInfo.getSerde());
        final Schema schema = new Schema();
        schema.setLocation(location);
        schema.setFields(toFields(tableInfo, schema));
        location.setSchema(schema);
        return location;
    }

    /**
     * Creates s3 partition.
     * @param table table
     * @param partitionInfo presto partition info
     * @return partition
     */
    public Partition toPartition(final Table table, final PartitionInfo partitionInfo) {
        final Partition result = fromPartitionInfo(partitionInfo);
        result.setTable(table);
        return result;
    }

    /**
     * Gets the partition uri.
     * @param partitionInfo partition
     * @return uri
     */
    public String getUri(final PartitionInfo partitionInfo) {
        return partitionInfo.getSerde() == null ? null : partitionInfo.getSerde().getUri();
    }

    /**
     * Gets the partition keys for the given table.
     * @param table table info
     * @return list of keys
     */
    public List<String> partitionKeys(final Table table) {
        List<String> result = Lists.newArrayList();
        if (table.getLocation() != null) {
            final Schema schema = table.getLocation().getSchema();
            if (schema != null) {
                final List<Field> fields = schema.getFields();
                result = fields.stream().filter(Field::isPartitionKey).map(Field::getName).collect(Collectors.toList());
            }
        }
        return result;
    }
}
