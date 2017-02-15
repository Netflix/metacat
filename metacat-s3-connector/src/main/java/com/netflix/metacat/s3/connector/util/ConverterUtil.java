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

package com.netflix.metacat.connector.s3.util;

import com.facebook.presto.spi.AuditInfo;
import com.facebook.presto.spi.ColumnDetailMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionDetail;
import com.facebook.presto.spi.ConnectorTableDetailMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.StorageInfo;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.converters.impl.PigTypeConverter;
import com.netflix.metacat.connector.s3.model.Field;
import com.netflix.metacat.connector.s3.model.Info;
import com.netflix.metacat.connector.s3.model.Location;
import com.netflix.metacat.connector.s3.model.Partition;
import com.netflix.metacat.connector.s3.model.Schema;
import com.netflix.metacat.connector.s3.model.Table;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * S3 Converter util.
 */
public class ConverterUtil {
    @Inject
    private Config config;
    @Inject
    private TypeManager typeManager;
    @Inject
    private PigTypeConverter pigTypeConverter;

    /**
     * Converts from s3 table info to presto storage info.
     * @param table table info
     * @return table info
     */
    public StorageInfo toStorageInfo(final Table table) {
        StorageInfo result = null;
        final Location location = table.getLocation();
        if (location != null) {
            result = new StorageInfo();
            result.setUri(location.getUri());
            final Info info = location.getInfo();
            if (info != null) {
                result.setInputFormat(info.getInputFormat());
                result.setOutputFormat(info.getOutputFormat());
                result.setSerializationLib(info.getSerializationLib());
                result.setSerdeInfoParameters(Maps.newHashMap(info.getParameters()));
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
     * @param owner owner
     * @return location
     */
    public Location fromStorageInfo(final StorageInfo storageInfo, final String owner) {
        final Location result = new Location();
        if (storageInfo != null) {
            result.setUri(storageInfo.getUri());
            final Info info = new Info();
            info.setLocation(result);
            info.setOwner(owner);
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
        } else if (owner != null) {
            final Info info = new Info();
            info.setLocation(result);
            info.setOwner(owner);
            result.setInfo(info);
        }
        return result;
    }

    /**
     * Creates list of fields from table info.
     * @param tableMetadata table info
     * @param schema schema
     * @return list of fields
     */
    public List<Field> toFields(final ConnectorTableMetadata tableMetadata, final Schema schema) {
        final ImmutableList.Builder<Field> columns = ImmutableList.builder();
        int index = 0;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            final Field field = toField(column);
            field.setPos(index++);
            field.setSchema(schema);
            columns.add(field);
        }
        return columns.build();
    }

    /**
     * Converts from column metadata to field.
     * @param column column
     * @return field
     */
    public Field toField(final ColumnMetadata column) {
        final Field result = new Field();
        result.setName(column.getName());
        result.setPartitionKey(column.isPartitionKey());
        result.setComment(column.getComment());
        result.setSourceType(column.getType().getDisplayName());
        result.setType(toTypeString(column.getType()));
        return result;
    }

    private String toTypeString(final Type type) {
        String result = null;
        if (config.isUsePigTypes()) {
            result = pigTypeConverter.fromType(type);
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
        if (config.isUsePigTypes()) {
            //Hack for now. We need to correct the type format in Franklin
            String typeString = type;
            if ("map".equals(type)) {
                typeString = "map[]";
            }
            result = pigTypeConverter.toType(typeString, typeManager);
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
        final AuditInfo result = new AuditInfo();
        final Location location = table.getLocation();
        if (location != null) {
            final Info info = location.getInfo();
            if (info != null) {
                result.setCreatedBy(info.getOwner());
                result.setLastUpdatedBy(info.getOwner());
            }
        }
        result.setCreatedDate(table.getCreatedDate() == null ? null : table.getCreatedDate().getTime() / 1000);
        result.setLastUpdatedDate(
            table.getLastUpdatedDate() == null ? null : table.getLastUpdatedDate().getTime() / 1000);
        return result;
    }

    /**
     * Create column.
     * @param field field
     * @return column
     */
    public ColumnMetadata toColumnMetadata(final Field field) {
        return new ColumnDetailMetadata(field.getName(), toType(field.getType()), field.isPartitionKey(),
            field.getComment(), false, field.getType());
    }

    /**
     * Create columns from the given table.
     * @param table table info
     * @return list of columns
     */
    public List<ColumnMetadata> toColumnMetadatas(final Table table) {
        List<ColumnMetadata> result = Lists.newArrayList();
        final Location location = table.getLocation();
        if (location != null) {
            final Schema schema = location.getSchema();
            if (schema != null) {
                result = schema.getFields().stream().sorted(Comparator.comparing(Field::getPos))
                    .map(this::toColumnMetadata).collect(Collectors.toList());
            }
        }
        return result;
    }

    /**
     * Creates location.
     * @param tableMetadata table info
     * @return location
     */
    public Location toLocation(final ConnectorTableMetadata tableMetadata) {
        Location location = null;
        if (tableMetadata instanceof ConnectorTableDetailMetadata) {
            final ConnectorTableDetailMetadata tableDetailMetadata = (ConnectorTableDetailMetadata) tableMetadata;
            location = fromStorageInfo(tableDetailMetadata.getStorageInfo(), tableDetailMetadata.getOwner());
        } else {
            location = new Location();
            final Info info = new Info();
            info.setLocation(location);
            info.setOwner(tableMetadata.getOwner());
            location.setInfo(info);
        }
        final Schema schema = new Schema();
        schema.setLocation(location);
        schema.setFields(toFields(tableMetadata, schema));
        location.setSchema(schema);
        return location;
    }

    /**
     * Creates s3 partition.
     * @param table table
     * @param partition presto partition info
     * @return partition
     */
    public Partition toPartition(final Table table, final ConnectorPartition partition) {
        final Partition result = new Partition();
        result.setTable(table);
        result.setName(partition.getPartitionId());
        result.setUri(getUri(partition));
        return result;
    }

    /**
     * Gets the partition uri.
     * @param partition partition
     * @return uri
     */
    public String getUri(final ConnectorPartition partition) {
        String result = null;
        if (partition instanceof ConnectorPartitionDetail) {
            final ConnectorPartitionDetail partitionDetail = (ConnectorPartitionDetail) partition;
            if (partitionDetail.getStorageInfo() != null) {
                result = partitionDetail.getStorageInfo().getUri();
            }
        }
        return result;
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
