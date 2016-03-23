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

package com.netflix.metacat.s3.connector.util;

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
import com.netflix.metacat.s3.connector.model.Field;
import com.netflix.metacat.s3.connector.model.Info;
import com.netflix.metacat.s3.connector.model.Location;
import com.netflix.metacat.s3.connector.model.Partition;
import com.netflix.metacat.s3.connector.model.Schema;
import com.netflix.metacat.s3.connector.model.Table;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by amajumdar on 2/4/15.
 */
public class ConverterUtil {
    @Inject
    private Config config;
    @Inject
    private TypeManager typeManager;
    @Inject
    private PigTypeConverter pigTypeConverter;
    public StorageInfo toStorageInfo(Table table){
        StorageInfo result = null;
        Location location = table.getLocation();
        if( location != null) {
            result = new StorageInfo();
            result.setUri( location.getUri());
            Info info = location.getInfo();
            if( info != null){
                result.setInputFormat(info.getInputFormat());
                result.setOutputFormat(info.getOutputFormat());
                result.setSerializationLib(info.getSerializationLib());
                result.setParameters(Maps.newHashMap(info.getParameters()));
            }
        }
        return result;
    }

    public String getOwner(Table table){
        String result = null;
        Location location = table.getLocation();
        if( location != null){
            Info info = location.getInfo();
            if(info != null){
                result = info.getOwner();
            }
        }
        return result;
    }

    public Location fromStorageInfo(StorageInfo storageInfo, String owner){
        Location result = new Location();
        if( storageInfo != null) {
            result.setUri( storageInfo.getUri());
            Info info = new Info();
            info.setLocation(result);
            info.setOwner(owner);
            info.setInputFormat( storageInfo.getInputFormat());
            info.setOutputFormat( storageInfo.getOutputFormat());
            info.setSerializationLib( storageInfo.getSerializationLib());
            info.setParameters( storageInfo.getParameters());
            result.setInfo(info);
        } else if ( owner != null){
            Info info = new Info();
            info.setLocation(result);
            info.setOwner(owner);
            result.setInfo(info);
        }
        return result;
    }

    public  List<Field> toFields(ConnectorTableMetadata tableMetadata, Schema schema) {
        ImmutableList.Builder<Field> columns = ImmutableList.builder();
        int index = 0;
        for( ColumnMetadata column: tableMetadata.getColumns()){
            Field field = toField( column);
            field.setPos(index++);
            field.setSchema(schema);
            columns.add( field);
        }
        return columns.build();
    }

    public Field toField(ColumnMetadata column) {
        Field result = new Field();
        result.setName( column.getName());
        result.setPartitionKey( column.isPartitionKey());
        result.setComment(column.getComment());
        result.setSourceType( column.getType().getDisplayName());
        result.setType(toTypeString( column.getType()));
        return result;
    }

    private String toTypeString(Type type) {
        String result = null;
        if (config.isUsePigTypes()){
            result = pigTypeConverter.fromType(type);
        } else {
            result = type.getDisplayName();
        }
        return result;
    }

    public Type toType(String type) {
        Type result = null;
        if (config.isUsePigTypes()) {
            //Hack for now. We need to correct the type format in Franklin
            if ("map".equals(type)) {
                type = "map[]";
            }
            result = pigTypeConverter.toType(type, typeManager);
        } else {
            result = typeManager.getType(TypeSignature.parseTypeSignature(type));
        }
        return result;
    }

    public AuditInfo toAuditInfo(Table table) {
        AuditInfo result = new AuditInfo();
        Location location = table.getLocation();
        if( location != null){
            Info info = location.getInfo();
            if( info != null) {
                result.setCreatedBy(info.getOwner());
                result.setLastUpdatedBy(info.getOwner());
            }
        }
        result.setCreatedDate(table.getCreatedDate()==null?null:table.getCreatedDate().getTime()/1000);
        result.setLastUpdatedDate(table.getLastUpdatedDate()==null?null:table.getLastUpdatedDate().getTime()/1000);
        return result;
    }

    public ColumnMetadata toColumnMetadata(Field field) {
        return new ColumnDetailMetadata(field.getName(), toType( field.getType()), field.isPartitionKey(), field.getComment(), false, field.getType());
    }

    public List<ColumnMetadata> toColumnMetadatas(Table table) {
        List<ColumnMetadata> result = Lists.newArrayList();
        Location location = table.getLocation();
        if( location != null){
            Schema schema = location.getSchema();
            if( schema != null){
                result = schema.getFields().stream().sorted(Comparator.comparing(Field::getPos)).map(this::toColumnMetadata).collect(Collectors.toList());
            }
        }
        return result;
    }

    public Location toLocation(ConnectorTableMetadata tableMetadata) {
        Location location = null;
        if( tableMetadata instanceof ConnectorTableDetailMetadata){
            ConnectorTableDetailMetadata tableDetailMetadata = (ConnectorTableDetailMetadata) tableMetadata;
            location = fromStorageInfo(tableDetailMetadata.getStorageInfo(), tableDetailMetadata.getOwner());
        } else {
            location = new Location();
            Info info = new Info();
            info.setLocation(location);
            info.setOwner( tableMetadata.getOwner());
            location.setInfo(info);
        }
        Schema schema = new Schema();
        schema.setLocation(location);
        schema.setFields(toFields(tableMetadata, schema));
        location.setSchema(schema);
        return location;
    }

    public Partition toPartition(Table table, ConnectorPartition partition) {
        Partition result = new Partition();
        result.setTable(table);
        result.setName(partition.getPartitionId());
        result.setUri(getUri(partition));
        return result;
    }

    public String getUri(ConnectorPartition partition) {
        String result = null;
        if( partition instanceof  ConnectorPartitionDetail) {
            ConnectorPartitionDetail partitionDetail = (ConnectorPartitionDetail) partition;
            if (partitionDetail.getStorageInfo() != null) {
                result = partitionDetail.getStorageInfo().getUri();
            }
        }
        return result;
    }


    public List<String> partitionKeys(Table table){
        List<String> result = Lists.newArrayList();
        if( table.getLocation() != null){
            Schema schema = table.getLocation().getSchema();
            if( schema != null){
                List<Field> fields = schema.getFields();
                result = fields.stream().filter(Field::isPartitionKey).map(Field::getName).collect(Collectors.toList());
            }
        }
        return result;
    }
}
