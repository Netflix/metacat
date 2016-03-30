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

package com.netflix.metacat.converters.impl;

import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.AuditDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.converters.HiveConverters;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.EMPTY_MAP;

@Mapper(uses = DateConverters.class,
        unmappedTargetPolicy = ReportingPolicy.ERROR,
        componentModel = "default")
public abstract class MapStructHiveConverters implements HiveConverters {
    private static final Splitter SLASH_SPLITTER = Splitter.on('/');
    private static final Splitter EQUAL_SPLITTER = Splitter.on('=').limit(2);

    @VisibleForTesting
    Integer dateToEpochSeconds(Date date) {
        if (date == null) {
            return null;
        }

        Instant instant = date.toInstant();
        long seconds = instant.getEpochSecond();
        if (seconds <= Integer.MAX_VALUE) {
            return (int) seconds;
        }

        throw new IllegalStateException("Unable to convert date " + date + " to an integer seconds value");
    }

    private Date epochSecondsToDate(long seconds) {
        Instant instant = Instant.ofEpochSecond(seconds);
        return Date.from(instant);
    }

    private FieldDto hiveToMetacatField(FieldSchema field, boolean isPartitionKey) {
        FieldDto dto = new FieldDto();
        dto.setName(field.getName());
        dto.setType(field.getType());
        dto.setSource_type(field.getType());
        dto.setComment(field.getComment());
        dto.setPartition_key(isPartitionKey);

        return dto;
    }

    private FieldSchema metacatToHiveField(FieldDto fieldDto) {
        FieldSchema result = new FieldSchema();
        result.setName(fieldDto.getName());
        result.setType(fieldDto.getType());
        result.setComment(fieldDto.getComment());
        return result;
    }

    @Override
    public TableDto hiveToMetacatTable(QualifiedName name, Table table) {
        TableDto dto = new TableDto();
        dto.setSerde(toStorageDto(table.getSd(), table.getOwner()));
        dto.setAudit(new AuditDto());
        dto.setName(name);
        if (table.isSetCreateTime()) {
            dto.getAudit().setCreatedDate(epochSecondsToDate(table.getCreateTime()));
        }
        dto.setMetadata(table.getParameters());

        List<FieldSchema> nonPartitionColumns = table.getSd().getCols();
        List<FieldSchema> partitionColumns = table.getPartitionKeys();
        List<FieldDto> allFields = Lists.newArrayListWithCapacity(nonPartitionColumns.size() + partitionColumns.size());
        nonPartitionColumns.stream()
                           .map(field -> this.hiveToMetacatField(field, false))
                           .forEachOrdered(allFields::add);
        partitionColumns.stream()
                        .map(field -> this.hiveToMetacatField(field, true))
                        .forEachOrdered(allFields::add);
        dto.setFields(allFields);

        return dto;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Database metacatToHiveDatabase(DatabaseDto dto) {
        Database database = new Database();

        String name = "";
        String description = "";
        QualifiedName databaseName = dto.getName();
        if (databaseName != null) {
            name = databaseName.getDatabaseName();
            // Since this is required setting it to the same as the DB name for now
            description = databaseName.getDatabaseName();
        }
        database.setName(name);
        database.setDescription(description);

        String dbUri = dto.getUri();
        if (Strings.isNullOrEmpty(dbUri)) {
            dbUri = "";
        }
        database.setLocationUri(dbUri);

        Map<String, String> metadata = dto.getMetadata();
        if (metadata == null) {
            metadata = EMPTY_MAP;
        }
        database.setParameters(metadata);

        return database;
    }

    @Override
    public Table metacatToHiveTable(TableDto dto) {
        Table table = new Table();
        String tableName = "";
        String databaseName = "";

        QualifiedName name = dto.getName();
        if (name != null) {
            tableName = name.getTableName();
            databaseName = name.getDatabaseName();
        }
        table.setTableName(tableName);
        table.setDbName(databaseName);

        StorageDto storageDto = dto.getSerde();
        String owner = "";
        if (storageDto != null && storageDto.getOwner() != null) {
            owner = storageDto.getOwner();
        }
        table.setOwner(owner);

        AuditDto auditDto = dto.getAudit();
        if (auditDto != null && auditDto.getCreatedDate() != null) {
            table.setCreateTime(dateToEpochSeconds(auditDto.getCreatedDate()));
        }

        Map<String, String> params = Collections.emptyMap();
        if (dto.getMetadata() != null) {
            params = dto.getMetadata();
        }
        table.setParameters(params);

        // TODO get this
        table.setTableType("EXTERNAL_TABLE");

        table.setSd(fromStorageDto(storageDto));
        StorageDescriptor sd = table.getSd();

        List<FieldDto> fields = dto.getFields();
        if (fields == null) {
            table.setPartitionKeys(Collections.emptyList());
            sd.setCols(Collections.emptyList());
        } else {
            List<FieldSchema> nonPartitionFields = Lists.newArrayListWithCapacity(fields.size());
            List<FieldSchema> partitionFields = Lists.newArrayListWithCapacity(fields.size());
            for (FieldDto fieldDto : fields) {
                FieldSchema f = metacatToHiveField(fieldDto);

                if (fieldDto.isPartition_key()) {
                    partitionFields.add(f);
                } else {
                    nonPartitionFields.add(f);
                }
            }
            table.setPartitionKeys(partitionFields);
            sd.setCols(nonPartitionFields);
        }

        return table;
    }

    private StorageDto toStorageDto(StorageDescriptor sd, String owner) {
        StorageDto result = new StorageDto();
        if (sd != null) {
            result.setOwner(owner);
            result.setUri(sd.getLocation());
            result.setInputFormat(sd.getInputFormat());
            result.setOutputFormat(sd.getOutputFormat());
            result.setParameters(sd.getParameters());
            SerDeInfo serde = sd.getSerdeInfo();
            if (serde != null) {
                result.setSerializationLib(serde.getSerializationLib());
                result.setSerdeInfoParameters(serde.getParameters());
            }
        }
        return result;
    }

    private StorageDescriptor fromStorageDto(StorageDto storageDto) {
        // Set all required fields to a non-null value
        StorageDescriptor result = new StorageDescriptor();
        String inputFormat = "";
        String location = "";
        String outputFormat = "";
        String serdeName = "";
        String serializationLib = "";
        Map<String, String> sdParams = EMPTY_MAP;
        Map<String, String> serdeParams = EMPTY_MAP;

        if (storageDto != null) {
            if (storageDto.getInputFormat() != null) {
                inputFormat = storageDto.getInputFormat();
            }
            if (storageDto.getUri() != null) {
                location = storageDto.getUri();
            }
            if (storageDto.getOutputFormat() != null) {
                outputFormat = storageDto.getOutputFormat();
            }
            if (storageDto.getSerializationLib() != null) {
                serializationLib = storageDto.getSerializationLib();
            }
            if (storageDto.getParameters() != null) {
                sdParams = storageDto.getParameters();
            }
            if (storageDto.getSerdeInfoParameters() != null) {
                serdeParams = storageDto.getSerdeInfoParameters();
            }
        }

        result.setInputFormat(inputFormat);
        result.setLocation(location);
        result.setOutputFormat(outputFormat);
        result.setSerdeInfo(new SerDeInfo(serdeName, serializationLib, serdeParams));
        result.setCols(Collections.emptyList());
        result.setBucketCols(Collections.emptyList());
        result.setSortCols(Collections.emptyList());
        result.setParameters(sdParams);
        return result;
    }

    @Override
    public PartitionDto hiveToMetacatPartition(TableDto tableDto, Partition partition) {
        QualifiedName tableName = tableDto.getName();
        QualifiedName partitionName = QualifiedName.ofPartition(tableName.getCatalogName(), tableName.getDatabaseName(),
                tableName.getTableName(), getNameFromPartVals(tableDto, partition.getValues()));

        PartitionDto result = new PartitionDto();
        String owner = "";
        if (tableDto.getSerde() != null) {
            owner = tableDto.getSerde().getOwner();
        }
        result.setSerde(toStorageDto(partition.getSd(), owner));
        result.setMetadata(partition.getParameters());

        AuditDto auditDto = new AuditDto();
        auditDto.setCreatedDate(epochSecondsToDate(partition.getCreateTime()));
        auditDto.setLastModifiedDate(epochSecondsToDate(partition.getLastAccessTime()));
        result.setAudit(auditDto);
        result.setName(partitionName);
        return result;
    }

    @Override
    public List<String> getPartValsFromName(TableDto tableDto, String partName) {
        // Unescape the partition name

        LinkedHashMap<String, String> hm = null;
        try {
            hm = Warehouse.makeSpecFromName(partName);
        } catch (MetaException e) {
            throw new IllegalArgumentException("Invalid partition name", e);
        }

        List<String> partVals = Lists.newArrayList();
        for (String key : tableDto.getPartition_keys()) {
            String val = hm.get(key);
            if (val == null) {
                throw new IllegalArgumentException("Invalid partition name - missing " + key);
            }
            partVals.add(val);
        }
        return partVals;
    }

    @Override
    public String getNameFromPartVals(TableDto tableDto, List<String> partVals) {
        List<String> partitionKeys = tableDto.getPartition_keys();
        if (partitionKeys.size() != partVals.size()) {
            throw new IllegalArgumentException("Not the same number of partition columns and partition values");
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < partitionKeys.size(); i++) {
            if (builder.length() > 0) {
                builder.append('/');
            }

            builder.append(partitionKeys.get(i));
            builder.append('=');
            builder.append(partVals.get(i));
        }
        return builder.toString();
    }

    @Override
    public Partition metacatToHivePartition(PartitionDto partitionDto, TableDto tableDto) {
        Partition result = new Partition();

        QualifiedName name = partitionDto.getName();
        List<String> values = Lists.newArrayListWithCapacity(16);
        String databaseName = "";
        String tableName = "";
        if (name != null) {
            if (name.getPartitionName() != null) {
                for (String partialPartName : SLASH_SPLITTER.split(partitionDto.getName().getPartitionName())) {
                    List<String> nameValues = ImmutableList.copyOf(EQUAL_SPLITTER.split(partialPartName));
                    if (nameValues.size() != 2) {
                        throw new IllegalStateException("Unrecognized partition name: " + partitionDto.getName());
                    }
                    String value = nameValues.get(1);
                    values.add(value);
                }
            }

            if (name.getDatabaseName() != null) {
                databaseName = name.getDatabaseName();
            }

            if (name.getTableName() != null) {
                tableName = name.getTableName();
            }
        }
        result.setValues(values);
        result.setDbName(databaseName);
        result.setTableName(tableName);

        Map<String, String> metadata = partitionDto.getMetadata();
        if (metadata == null) {
            metadata = EMPTY_MAP;
        }
        result.setParameters(metadata);

        result.setSd(fromStorageDto(partitionDto.getSerde()));
        StorageDescriptor sd = result.getSd();
        if (tableDto != null) {
            if (sd.getSerdeInfo() != null && tableDto.getSerde() != null && Strings.isNullOrEmpty(
                    sd.getSerdeInfo().getSerializationLib())) {
                sd.getSerdeInfo().setSerializationLib(tableDto.getSerde().getSerializationLib());
            }

            List<FieldDto> fields = tableDto.getFields();
            if (fields == null) {
                sd.setCols(Collections.emptyList());
            } else {
                sd.setCols(fields.stream()
                                 .filter(field -> !field.isPartition_key())
                                 .map(this::metacatToHiveField)
                                 .collect(Collectors.toList()));
            }
        }

        AuditDto auditDto = partitionDto.getAudit();
        if (auditDto != null) {
            if (auditDto.getCreatedDate() != null) {
                result.setCreateTime(dateToEpochSeconds(auditDto.getCreatedDate()));
            }
            if (auditDto.getLastModifiedDate() != null) {
                result.setLastAccessTime(dateToEpochSeconds(auditDto.getLastModifiedDate()));
            }
        }

        return result;
    }
}
