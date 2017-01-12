/*
 *
 *  Copyright 2016 Netflix, Inc.
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

package com.netflix.metacat.hive.canonical.converters;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Class implements hiveMetacatConverter interface.
 */
public class HiveMetacatConvertersImpl implements HiveMetacatConverters {
    private static final Splitter SLASH_SPLITTER = Splitter.on('/');
    private static final Splitter EQUAL_SPLITTER = Splitter.on('=').limit(2);

    @VisibleForTesting
    Integer dateToEpochSeconds(final Date date) {
        return null == date ? null : Math.toIntExact(date.toInstant().getEpochSecond());
    }

    private Date epochSecondsToDate(final long seconds) {
        return Date.from(Instant.ofEpochSecond(seconds));
    }

    private FieldDto hiveToMetacatField(final FieldSchema field, final boolean isPartitionKey) {
        return FieldDto.builder().name(field.getName())
            .type(field.getType())
            .source_type(field.getType())
            .comment(field.getComment())
            .partition_key(isPartitionKey)
            .build();
    }

    private FieldSchema metacatToHiveField(final FieldDto fieldDto) {
        final FieldSchema result = new FieldSchema();
        result.setName(fieldDto.getName());
        result.setType(fieldDto.getType());
        result.setComment(fieldDto.getComment());
        return result;
    }

    @Override
    public TableDto hiveToMetacatTable(final QualifiedName name, final Table table) {
        final List<FieldSchema> nonPartitionColumns = table.getSd().getCols();
        final List<FieldSchema> partitionColumns = table.getPartitionKeys();
        final List<FieldDto> allFields =
            Lists.newArrayListWithCapacity(nonPartitionColumns.size() + partitionColumns.size());
        nonPartitionColumns.stream()
            .map(field -> this.hiveToMetacatField(field, false))
            .forEachOrdered(allFields::add);
        partitionColumns.stream()
            .map(field -> this.hiveToMetacatField(field, true))
            .forEachOrdered(allFields::add);
        final TableDto dto = TableDto.builder().serde(toStorageDto(table.getSd(), table.getOwner()))
            .audit(new AuditDto())
            .name(name)
            .metadata(table.getParameters())
            .fields(allFields)
            .build();
        if (table.isSetCreateTime()) {
            dto.getAudit().setCreatedDate(epochSecondsToDate(table.getCreateTime()));
        }
        return dto;
    }

    @Override
    public Database metacatToHiveDatabase(final DatabaseDto dto) {
        final QualifiedName databaseName = dto.getName();
        final String name = (databaseName == null) ? "" : databaseName.getDatabaseName();
        final String dbUri = Strings.isNullOrEmpty(dto.getUri()) ? "" : dto.getUri();
        final Map<String, String> metadata = (dto.getMetadata() != null) ? dto.getMetadata() : Collections.EMPTY_MAP;
        //setting the description as name for now
        return new Database(name, name, dbUri, metadata);
    }

    @Override
    public Table metacatToHiveTable(final TableDto dto) {
        final QualifiedName name = dto.getName();
        final String tableName = (name != null) ? name.getTableName() : "";
        final String databaseName = (name != null) ? name.getDatabaseName() : "";

        final StorageDto storageDto = dto.getSerde();
        String owner = "";
        if (storageDto != null && storageDto.getOwner() != null) {
            owner = storageDto.getOwner();
        }

        final AuditDto auditDto = dto.getAudit();
        int createTime = 0;
        if (auditDto != null && auditDto.getCreatedDate() != null) {
            createTime = dateToEpochSeconds(auditDto.getCreatedDate());
        }

        final Map<String, String> params = (dto.getMetadata() != null)
            ? dto.getMetadata() : Collections.emptyMap();
        final StorageDescriptor sd = fromStorageDto(storageDto);
        final List<FieldDto> fields = dto.getFields();
        List<FieldSchema> partitionFields = Collections.emptyList();
        List<FieldSchema> nonPartitionFields = Collections.emptyList();
        if (fields != null) {
            nonPartitionFields = Lists.newArrayListWithCapacity(fields.size());
            partitionFields = Lists.newArrayListWithCapacity(fields.size());
            for (FieldDto fieldDto : fields) {
                if (fieldDto.isPartition_key()) {
                    partitionFields.add(metacatToHiveField(fieldDto));
                } else {
                    nonPartitionFields.add(metacatToHiveField(fieldDto));
                }
            }
        }
        sd.setCols(nonPartitionFields);

        return new Table(tableName,
            databaseName,
            owner,
            createTime,
            0,
            0,
            sd,
            partitionFields,
            params,
            "",
            "",
            "EXTERNAL_TABLE");
    }

    private StorageDto toStorageDto(final StorageDescriptor sd, final String owner) {
        if (sd == null) {
            return new StorageDto();
        }
        final StorageDto result = StorageDto.builder().owner(owner)
            .uri(sd.getLocation())
            .inputFormat(sd.getInputFormat())
            .outputFormat(sd.getOutputFormat())
            .parameters(sd.getParameters())
            .build();
        if (sd.getSerdeInfo() != null) {
            result.setSerializationLib(sd.getSerdeInfo().getSerializationLib());
            result.setSerdeInfoParameters(sd.getSerdeInfo().getParameters());
        }
        return result;
    }

    private StorageDescriptor fromStorageDto(final StorageDto storageDto) {
        // Set all required fields to a non-null value
        String inputFormat = "";
        String location = "";
        String outputFormat = "";
        final String serdeName = "";
        String serializationLib = "";
        Map<String, String> sdParams = Collections.emptyMap();
        Map<String, String> serdeParams = Collections.emptyMap();

        if (notNull(storageDto)) {
            if (notNull(storageDto.getInputFormat())) {
                inputFormat = storageDto.getInputFormat();
            }
            if (notNull(storageDto.getUri())) {
                location = storageDto.getUri();
            }
            if (notNull(storageDto.getOutputFormat())) {
                outputFormat = storageDto.getOutputFormat();
            }
            if (notNull(storageDto.getSerializationLib())) {
                serializationLib = storageDto.getSerializationLib();
            }
            if (notNull(storageDto.getParameters())) {
                sdParams = storageDto.getParameters();
            }
            if (notNull(storageDto.getSerdeInfoParameters())) {
                serdeParams = storageDto.getSerdeInfoParameters();
            }
        }
        return new StorageDescriptor(
            Collections.emptyList(),
            location,
            inputFormat,
            outputFormat,
            false,
            0,
            new SerDeInfo(serdeName, serializationLib, serdeParams),
            Collections.emptyList(),
            Collections.emptyList(),
            sdParams);
    }

    @Override
    public PartitionDto hiveToMetacatPartition(final TableDto tableDto, final Partition partition) {
        final QualifiedName tableName = tableDto.getName();
        final QualifiedName partitionName = QualifiedName.ofPartition(tableName.getCatalogName(),
            tableName.getDatabaseName(),
            tableName.getTableName(),
            getNameFromPartVals(tableDto, partition.getValues()));
        final String owner = notNull(tableDto.getSerde()) ? tableDto.getSerde().getOwner() : "";
        final AuditDto auditDto = AuditDto.builder()
            .createdDate(epochSecondsToDate(partition.getCreateTime()))
            .lastModifiedDate(epochSecondsToDate(partition.getLastAccessTime())).build();

        return PartitionDto.builder()
            .name(partitionName)
            .audit(auditDto)
            .serde(toStorageDto(partition.getSd(), owner))
            .metadata(partition.getParameters())
            .build();
    }

    @Override
    public List<String> getPartValsFromName(final TableDto tableDto, final String partName) {
        // Unescape the partition name
        LinkedHashMap<String, String> hm = null;
        try {
            hm = Warehouse.makeSpecFromName(partName);
        } catch (MetaException e) {
            throw new IllegalArgumentException("Invalid partition name", e);
        }

        final List<String> partVals = Lists.newArrayList();
        for (String key : tableDto.getPartition_keys()) {
            Preconditions.checkArgument(notNull(hm.get(key)), "Invalid partition name - missing " + key);
            partVals.add(hm.get(key));
        }
        return partVals;
    }

    @Override
    public String getNameFromPartVals(final TableDto tableDto, final List<String> partVals) {
        final List<String> partitionKeys = tableDto.getPartition_keys();
        if (partitionKeys.size() != partVals.size()) {
            throw new IllegalArgumentException("Not the same number of partition columns and partition values");
        }
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < partitionKeys.size(); i++) {
            if (builder.length() > 0) {
                builder.append('/');
            }
            builder.append(partitionKeys.get(i))
                .append('=')
                .append(partVals.get(i));
        }
        return builder.toString();
    }

    @Override
    public Partition metacatToHivePartition(final PartitionDto partitionDto, final TableDto tableDto) {
        final QualifiedName name = partitionDto.getName();
        final List<String> values = Lists.newArrayListWithCapacity(16);
        String databaseName = "";
        String tableName = "";
        if (notNull(name)) {
            if (notNull(name.getPartitionName())) {
                for (String partialPartName : SLASH_SPLITTER.split(partitionDto.getName().getPartitionName())) {
                    final List<String> nameValues = ImmutableList.copyOf(EQUAL_SPLITTER.split(partialPartName));
                    Preconditions.checkState(nameValues.size() == 2,
                        "Unrecognized partition name: " + partitionDto.getName());
                    values.add(nameValues.get(1));
                }
            }
            if (notNull(name.getDatabaseName())) {
                databaseName = name.getDatabaseName();
            }
            if (notNull(name.getTableName())) {
                tableName = name.getTableName();
            }
        }

        Map<String, String> metadata = partitionDto.getMetadata();
        if (metadata == null) {
            metadata = Collections.emptyMap();
        }

        final StorageDescriptor sd = fromStorageDto(partitionDto.getSerde());
        if (notNull(tableDto)) {
            if (notNull(sd.getSerdeInfo())
                && notNull(tableDto.getSerde())
                && Strings.isNullOrEmpty(sd.getSerdeInfo().getSerializationLib())) {
                sd.getSerdeInfo().setSerializationLib(tableDto.getSerde().getSerializationLib());
            }
            final List<FieldDto> fields = tableDto.getFields();
            if (notNull(fields)) {
                sd.setCols(fields.stream()
                    .filter(field -> !field.isPartition_key())
                    .map(this::metacatToHiveField)
                    .collect(Collectors.toList()));
            } else {
                sd.setCols(Collections.emptyList());
            }
        }

        final AuditDto auditDto = partitionDto.getAudit();
        final int createTime = (notNull(auditDto) && notNull(auditDto.getCreatedDate()))
            ? dateToEpochSeconds(auditDto.getCreatedDate()) : 0;
        final int lastAccessTime = (notNull(auditDto) && notNull(auditDto.getLastModifiedDate()))
            ? dateToEpochSeconds(auditDto.getLastModifiedDate()) : 0;

        return new Partition(
            values,
            databaseName,
            tableName,
            createTime,
            lastAccessTime,
            sd,
            metadata);

    }

    private boolean notNull(final Object object) {
        return null != object;
    }
}
