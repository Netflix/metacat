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
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by amajumdar on 2/4/15.
 */
public class ConverterUtil {
    @Inject
    HiveTypeConverter hiveTypeConverter;
    private static final Logger log = LoggerFactory.getLogger(ConverterUtil.class);

    public StorageInfo toStorageInfo(StorageDescriptor sd){
        StorageInfo result = null;
        if( sd != null) {
            result = new StorageInfo();
            result.setUri( sd.getLocation());
            result.setInputFormat(sd.getInputFormat());
            result.setOutputFormat(sd.getOutputFormat());
            SerDeInfo serde = sd.getSerdeInfo();
            if( serde != null){
                result.setSerializationLib(serde.getSerializationLib());
                result.setSerdeInfoParameters(serde.getParameters());
            }
            result.setParameters(sd.getParameters());
        }
        return result;
    }

    public StorageDescriptor fromStorageInfo(StorageInfo storageInfo){
        StorageDescriptor result = null;
        if( storageInfo != null) {
            result = new StorageDescriptor();
            result.setInputFormat( storageInfo.getInputFormat());
            result.setLocation(storageInfo.getUri());
            result.setOutputFormat(storageInfo.getOutputFormat());
            result.setParameters(storageInfo.getParameters());
            result.setSerdeInfo( new SerDeInfo(null, storageInfo.getSerializationLib(), storageInfo.getSerdeInfoParameters()));
        }
        return result;
    }

    public List<FieldSchema> toFieldSchemas(ConnectorTableDetailMetadata tableDetailMetadata) {
        ImmutableList.Builder<FieldSchema> columns = ImmutableList.builder();
        for( ColumnMetadata column: tableDetailMetadata.getColumns()){
            columns.add( toFieldSchema( column));
        }
        return columns.build();
    }

    public FieldSchema toFieldSchema(ColumnMetadata column) {
        return new FieldSchema(column.getName(), hiveTypeConverter.fromType(column.getType()), column.getComment());
    }

    public AuditInfo toAuditInfo(Table table) {
        AuditInfo result = new AuditInfo();
        result.setCreatedBy(table.getOwner());
        result.setCreatedDate((long) table.getCreateTime());
        Map<String, String> parameters = table.getParameters();
        if( parameters != null) {
            result.setLastUpdatedBy(parameters.get("last_modified_by"));
            Long lastModifiedDate = null;
            try{
                lastModifiedDate = Long.valueOf(parameters.get("last_modified_time"));
            }catch(Exception ignored){

            }
            result.setLastUpdatedDate(lastModifiedDate);
        }
        return result;
    }

    public Optional<ColumnMetadata> toColumnMetadata(FieldSchema field, TypeManager typeManager, int index, boolean isPartitionKey) {
        String fieldType = field.getType();
        Type type = hiveTypeConverter.toType(fieldType, typeManager);
        if (type == null) {
            log.debug("Unable to convert type '{}' for field '{}' to a hive type", fieldType, field.getName());
            return Optional.empty();
        }
        ColumnDetailMetadata metadata = new ColumnDetailMetadata(field.getName(), type, isPartitionKey,
                field.getComment(), false, fieldType);
        return Optional.of(metadata);
    }

    public List<ColumnMetadata> toColumnMetadatas(Table table, TypeManager typeManager) {
        List<ColumnMetadata> result = Lists.newArrayList();
        StorageDescriptor sd = table.getSd();
        int index = 0;
        if( sd != null) {
            List<FieldSchema> fields = table.getSd().getCols();
            for (FieldSchema field : fields) {
                Optional<ColumnMetadata> columnMetadata = toColumnMetadata(field, typeManager, index, false);
                // Ignore unsupported types rather than failing
                if (columnMetadata.isPresent()) {
                    index++;
                    result.add(columnMetadata.get());
                }
            }
        }
        List<FieldSchema> pFields = table.getPartitionKeys();
        if( pFields != null) {
            for (FieldSchema pField : pFields) {
                Optional<ColumnMetadata> columnMetadata = toColumnMetadata(pField, typeManager, index, true);
                // Ignore unsupported types rather than failing
                if (columnMetadata.isPresent()) {
                    index++;
                    result.add(columnMetadata.get());
                }
            }
        }
        return result;
    }

    public List<Partition> toPartitions(SchemaTableName tableName, List<ConnectorPartition> partitions) {
        return partitions.stream().map(partition -> toPartition(tableName, partition)).collect(
                Collectors.toList());
    }

    public Partition toPartition(SchemaTableName tableName, ConnectorPartition connectorPartition) {
        Partition result = new Partition();
        ConnectorPartitionDetail connectorPartitionDetail = (ConnectorPartitionDetail) connectorPartition;
        result.setValues(Lists.newArrayList(
                PartitionUtil.getPartitionKeyValues(connectorPartitionDetail.getPartitionId()).values()));
        result.setDbName( tableName.getSchemaName());
        result.setTableName( tableName.getTableName());
        result.setSd(fromStorageInfo(connectorPartitionDetail.getStorageInfo()));
        result.setParameters(connectorPartitionDetail.getMetadata());
        AuditInfo auditInfo = connectorPartitionDetail.getAuditInfo();
        if( auditInfo != null){
            Long createdDate = auditInfo.getCreatedDate();
            int currentTime = (int) (System.currentTimeMillis() / 1000);
            if( createdDate != null){
                result.setCreateTime( createdDate.intValue());
            } else {
                result.setCreateTime(currentTime);
            }
            Long lastUpdatedDate = auditInfo.getLastUpdatedDate();
            if( lastUpdatedDate != null){
                result.setLastAccessTime( lastUpdatedDate.intValue());
            }else {
                result.setLastAccessTime(currentTime);
            }
        }
        return result;
    }
}
