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

package com.netflix.metacat.hive.connector.util;

import com.facebook.presto.spi.AuditInfo;
import com.facebook.presto.spi.ColumnDetailMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionDetail;
import com.facebook.presto.spi.ConnectorTableDetailMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StorageInfo;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.partition.util.PartitionUtil;
import com.netflix.metacat.converters.impl.HiveTypeConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Hive converter utility.
 */
@Slf4j
public class ConverterUtil {
    /** Hive type converter. */
    @Inject
    private HiveTypeConverter hiveTypeConverter;

    /**
     * Converts from hive storage info to presto storage info.
     * @param sd storage descriptor
     * @return StorageInfo
     */
    public StorageInfo toStorageInfo(final StorageDescriptor sd) {
        StorageInfo result = null;
        if (sd != null) {
            result = new StorageInfo();
            result.setUri(sd.getLocation());
            result.setInputFormat(sd.getInputFormat());
            result.setOutputFormat(sd.getOutputFormat());
            final SerDeInfo serde = sd.getSerdeInfo();
            if (serde != null) {
                result.setSerializationLib(serde.getSerializationLib());
                result.setSerdeInfoParameters(serde.getParameters());
            }
            result.setParameters(sd.getParameters());
        }
        return result;
    }

    /**
     * Converts from presto storage info.
     * @param storageInfo storage info
     * @return StorageDescriptor
     */
    public StorageDescriptor fromStorageInfo(final StorageInfo storageInfo) {
        StorageDescriptor result = null;
        if (storageInfo != null) {
            result = new StorageDescriptor();
            result.setInputFormat(storageInfo.getInputFormat());
            result.setLocation(storageInfo.getUri());
            result.setOutputFormat(storageInfo.getOutputFormat());
            result.setParameters(storageInfo.getParameters());
            result.setSerdeInfo(
                new SerDeInfo(null, storageInfo.getSerializationLib(), storageInfo.getSerdeInfoParameters()));
        }
        return result;
    }

    /**
     * Creates list of hive fields.
     * @param tableDetailMetadata table info
     * @return list of fields
     */
    public List<FieldSchema> toFieldSchemas(final ConnectorTableDetailMetadata tableDetailMetadata) {
        final ImmutableList.Builder<FieldSchema> columns = ImmutableList.builder();
        for (ColumnMetadata column : tableDetailMetadata.getColumns()) {
            columns.add(toFieldSchema(column));
        }
        return columns.build();
    }

    /**
     * Converts from presto column to hive field.
     * @param column column
     * @return field
     */
    public FieldSchema toFieldSchema(final ColumnMetadata column) {
        return new FieldSchema(column.getName(), hiveTypeConverter.fromType(column.getType()), column.getComment());
    }

    /**
     * Creates audit info from Table dto..
     * @param table table info
     * @return audit info
     */
    public AuditInfo toAuditInfo(final Table table) {
        final AuditInfo result = new AuditInfo();
        result.setCreatedBy(table.getOwner());
        result.setCreatedDate((long) table.getCreateTime());
        final Map<String, String> parameters = table.getParameters();
        if (parameters != null) {
            result.setLastUpdatedBy(parameters.get("last_modified_by"));
            Long lastModifiedDate = null;
            try {
                lastModifiedDate = Long.valueOf(parameters.get("last_modified_time"));
            } catch (Exception ignored) {

            }
            result.setLastUpdatedDate(lastModifiedDate);
        }
        return result;
    }

    /**
     * Converts from hive field to column metadata.
     * @param field field
     * @param typeManager manager
     * @param index index
     * @param isPartitionKey is it a partition key
     * @return column metadata
     */
    public Optional<ColumnMetadata> toColumnMetadata(final FieldSchema field, final TypeManager typeManager,
        final int index, final boolean isPartitionKey) {
        final String fieldType = field.getType();
        final Type type = hiveTypeConverter.toType(fieldType, typeManager);
        if (type == null) {
            log.debug("Unable to convert type '{}' for field '{}' to a hive type", fieldType, field.getName());
            return Optional.empty();
        }
        final ColumnDetailMetadata metadata = new ColumnDetailMetadata(field.getName(), type, isPartitionKey,
            field.getComment(), false, fieldType);
        return Optional.of(metadata);
    }

    /**
     * Creates a list of columns from hive table.
     * @param table hive table
     * @param typeManager manager
     * @return list of columns
     */
    public List<ColumnMetadata> toColumnMetadatas(final Table table, final TypeManager typeManager) {
        final List<ColumnMetadata> result = Lists.newArrayList();
        final StorageDescriptor sd = table.getSd();
        int index = 0;
        if (sd != null) {
            final List<FieldSchema> fields = table.getSd().getCols();
            for (FieldSchema field : fields) {
                final Optional<ColumnMetadata> columnMetadata = toColumnMetadata(field, typeManager, index, false);
                // Ignore unsupported types rather than failing
                if (columnMetadata.isPresent()) {
                    index++;
                    result.add(columnMetadata.get());
                }
            }
        }
        final List<FieldSchema> pFields = table.getPartitionKeys();
        if (pFields != null) {
            for (FieldSchema pField : pFields) {
                final Optional<ColumnMetadata> columnMetadata = toColumnMetadata(pField, typeManager, index, true);
                // Ignore unsupported types rather than failing
                if (columnMetadata.isPresent()) {
                    index++;
                    result.add(columnMetadata.get());
                }
            }
        }
        return result;
    }

    /**
     * Converts from a list of presto partitions to hive partitions.
     * @param tableName table name
     * @param partitions list of partitions
     * @return list of hive partitions
     */
    public List<Partition> toPartitions(final SchemaTableName tableName, final List<ConnectorPartition> partitions) {
        return partitions.stream().map(partition -> toPartition(tableName, partition)).collect(
            Collectors.toList());
    }

    /**
     * Converts from a presto partition to hive partition.
     * @param tableName table name
     * @param connectorPartition partition
     * @return hive partition
     */
    public Partition toPartition(final SchemaTableName tableName, final ConnectorPartition connectorPartition) {
        final Partition result = new Partition();
        final ConnectorPartitionDetail connectorPartitionDetail = (ConnectorPartitionDetail) connectorPartition;
        result.setValues(Lists.newArrayList(
            PartitionUtil.getPartitionKeyValues(connectorPartitionDetail.getPartitionId()).values()));
        result.setDbName(tableName.getSchemaName());
        result.setTableName(tableName.getTableName());
        result.setSd(fromStorageInfo(connectorPartitionDetail.getStorageInfo()));
        result.setParameters(connectorPartitionDetail.getMetadata());
        final AuditInfo auditInfo = connectorPartitionDetail.getAuditInfo();
        if (auditInfo != null) {
            final Long createdDate = auditInfo.getCreatedDate();
            final int currentTime = (int) (System.currentTimeMillis() / 1000);
            if (createdDate != null) {
                result.setCreateTime(createdDate.intValue());
            } else {
                result.setCreateTime(currentTime);
            }
            final Long lastUpdatedDate = auditInfo.getLastUpdatedDate();
            if (lastUpdatedDate != null) {
                result.setLastAccessTime(lastUpdatedDate.intValue());
            } else {
                result.setLastAccessTime(currentTime);
            }
        }
        return result;
    }
}
