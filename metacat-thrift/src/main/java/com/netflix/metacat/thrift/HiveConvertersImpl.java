/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.metacat.thrift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.AuditDto;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.dto.ViewDto;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hive converter.
 */
public class HiveConvertersImpl implements HiveConverters {

    @VisibleForTesting
    Integer dateToEpochSeconds(@Nullable final Date date) {
        if (date == null) {
            return null;
        }

        final Instant instant = date.toInstant();
        final long seconds = instant.getEpochSecond();
        if (seconds <= Integer.MAX_VALUE) {
            return (int) seconds;
        }

        throw new IllegalStateException("Unable to convert date " + date + " to an integer seconds value");
    }

    private Date epochSecondsToDate(final long seconds) {
        final Instant instant = Instant.ofEpochSecond(seconds);
        return Date.from(instant);
    }

    private FieldDto hiveToMetacatField(final FieldSchema field, final boolean isPartitionKey) {
        final FieldDto dto = new FieldDto();
        dto.setName(field.getName());
        dto.setType(field.getType());
        dto.setSource_type(field.getType());
        dto.setComment(field.getComment());
        dto.setPartition_key(isPartitionKey);

        return dto;
    }

    private FieldSchema metacatToHiveField(final FieldDto fieldDto) {
        final FieldSchema result = new FieldSchema();
        result.setName(fieldDto.getName());
        result.setType(fieldDto.getType());
        result.setComment(fieldDto.getComment());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TableDto hiveToMetacatTable(final QualifiedName name, final Table table) {
        final TableDto dto = new TableDto();
        dto.setSerde(toStorageDto(table.getSd(), table.getOwner()));
        dto.setAudit(new AuditDto());
        dto.setName(name);
        if (table.isSetCreateTime()) {
            dto.getAudit().setCreatedDate(epochSecondsToDate(table.getCreateTime()));
        }
        dto.setMetadata(table.getParameters());

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
        dto.setFields(allFields);
        dto.setView(new ViewDto(table.getViewOriginalText(),
            table.getViewExpandedText()));
        return dto;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public Database metacatToHiveDatabase(final DatabaseDto dto) {
        final Database database = new Database();

        String name = "";
        String description = "";
        final QualifiedName databaseName = dto.getName();
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
            metadata = Collections.EMPTY_MAP;
        }
        database.setParameters(metadata);

        return database;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table metacatToHiveTable(final TableDto dto) {
        final Table table = new Table();
        final QualifiedName name = dto.getName();
        if (name != null) {
            table.setTableName(name.getTableName());
            table.setDbName(name.getDatabaseName());
        }

        final StorageDto storageDto = dto.getSerde();
        if (storageDto != null) {
            table.setOwner(storageDto.getOwner());
        }

        final AuditDto auditDto = dto.getAudit();
        if (auditDto != null && auditDto.getCreatedDate() != null) {
            table.setCreateTime(dateToEpochSeconds(auditDto.getCreatedDate()));
        }

        Map<String, String> params = new HashMap<>();
        if (dto.getMetadata() != null) {
            params = dto.getMetadata();
        }
        table.setParameters(params);
        updateTableTypeAndViewInfo(dto, table);

        table.setSd(fromStorageDto(storageDto, table.getTableName()));

        final List<FieldDto> fields = dto.getFields();
        if (fields == null) {
            table.setPartitionKeys(Collections.emptyList());
            table.getSd().setCols(Collections.emptyList());
        } else {
            final List<FieldSchema> nonPartitionFields = Lists.newArrayListWithCapacity(fields.size());
            final List<FieldSchema> partitionFields = Lists.newArrayListWithCapacity(fields.size());
            for (FieldDto fieldDto : fields) {
                final FieldSchema f = metacatToHiveField(fieldDto);

                if (fieldDto.isPartition_key()) {
                    partitionFields.add(f);
                } else {
                    nonPartitionFields.add(f);
                }
            }
            table.setPartitionKeys(partitionFields);
            table.getSd().setCols(nonPartitionFields);
        }
        return table;
    }

    private void updateTableTypeAndViewInfo(final TableDto dto, final Table table) {
        final ViewDto viewDto = dto.getView();
        if (null == dto.getView() || Strings.isNullOrEmpty(viewDto.getViewOriginalText())) {
            table.setTableType(TableType.EXTERNAL_TABLE.name());
            return;
        }
        table.setTableType(TableType.VIRTUAL_VIEW.name());
        table.setViewOriginalText(viewDto.getViewOriginalText());
        table.setViewExpandedText(viewDto.getViewExpandedText());
    }

    private StorageDto toStorageDto(@Nullable final StorageDescriptor sd, final String owner) {
        final StorageDto result = new StorageDto();
        if (sd != null) {
            result.setOwner(owner);
            result.setUri(sd.getLocation());
            result.setInputFormat(sd.getInputFormat());
            result.setOutputFormat(sd.getOutputFormat());
            result.setParameters(sd.getParameters());
            final SerDeInfo serde = sd.getSerdeInfo();
            if (serde != null) {
                result.setSerializationLib(serde.getSerializationLib());
                result.setSerdeInfoParameters(serde.getParameters());
            }
        }
        return result;
    }

    private StorageDescriptor fromStorageDto(@Nullable final StorageDto storageDto, @Nullable final String serdeName) {
        //
        // Set all required fields to null. This is to simulate Hive behavior.
        // Setting it to empty string failed certain hive operations.
        //
        final StorageDescriptor result = new StorageDescriptor();
        String inputFormat = null;
        String location = null;
        String outputFormat = null;
        String serializationLib = null;
        Map<String, String> sdParams = Maps.newHashMap();
        Map<String, String> serdeParams = Maps.newHashMap();

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
        result.setSerdeInfo(new SerDeInfo(serdeName, serializationLib, serdeParams));
        result.setBucketCols(Collections.emptyList());
        result.setSortCols(Collections.emptyList());
        result.setInputFormat(inputFormat);
        result.setLocation(location);
        result.setOutputFormat(outputFormat);
        result.setCols(Collections.emptyList());
        // Setting an empty skewed info.
        result.setSkewedInfo(new SkewedInfo(Collections.emptyList(), Collections.emptyList(), Collections.emptyMap()));
        result.setParameters(sdParams);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PartitionDto hiveToMetacatPartition(final TableDto tableDto, final Partition partition) {
        final QualifiedName tableName = tableDto.getName();
        final QualifiedName partitionName = QualifiedName.ofPartition(tableName.getCatalogName(),
            tableName.getDatabaseName(),
            tableName.getTableName(), getNameFromPartVals(tableDto, partition.getValues()));

        final PartitionDto result = new PartitionDto();
        String owner = "";
        if (tableDto.getSerde() != null) {
            owner = tableDto.getSerde().getOwner();
        }
        //not setting Serde to view
        if (null == tableDto.getView() || Strings.isNullOrEmpty(tableDto.getView().getViewOriginalText())) {
            result.setSerde(toStorageDto(partition.getSd(), owner));
        }
        result.setMetadata(partition.getParameters());

        final AuditDto auditDto = new AuditDto();
        auditDto.setCreatedDate(epochSecondsToDate(partition.getCreateTime()));
        auditDto.setLastModifiedDate(epochSecondsToDate(partition.getLastAccessTime()));
        result.setAudit(auditDto);
        result.setName(partitionName);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getPartValsFromName(@Nullable final TableDto tableDto, final String partName) {
        // Unescape the partition name

        final LinkedHashMap<String, String> hm;
        try {
            hm = Warehouse.makeSpecFromName(partName);
        } catch (MetaException e) {
            throw new IllegalArgumentException("Invalid partition name", e);
        }
        // Get the partition keys.
        List<String> partitionKeys = null;
        if (tableDto != null) {
            partitionKeys = tableDto.getPartition_keys();
        }
        // If table has not been provided, return the values without validating.
        if (partitionKeys != null) {
            final List<String> partVals = Lists.newArrayListWithCapacity(partitionKeys.size());
            for (String key : partitionKeys) {
                final String val = hm.get(key);
                if (val == null) {
                    throw new IllegalArgumentException("Invalid partition name - missing " + key);
                }
                partVals.add(val);
            }
            return partVals;
        } else {
            return Lists.newArrayList(hm.values());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getNameFromPartVals(final TableDto tableDto, final List<String> partVals) {
        final List<String> partitionKeys = tableDto.getPartition_keys();
        if (partitionKeys.size() != partVals.size()) {
            throw new IllegalArgumentException("Not the same number of partition columns and partition values");
        }
        return FileUtils.makePartName(partitionKeys, partVals, "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition metacatToHivePartition(final PartitionDto partitionDto, @Nullable final TableDto tableDto) {
        final Partition result = new Partition();

        final QualifiedName name = partitionDto.getName();
        List<String> values = Lists.newArrayListWithCapacity(16);
        String databaseName = null;
        String tableName = null;
        if (name != null) {
            if (name.getPartitionName() != null) {
                //
                // Unescape the partition name to get the right partition values.
                // Partition name always are escaped where as the parition values are not.
                //
                values = getPartValsFromName(tableDto, name.getPartitionName());
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
            metadata = Maps.newHashMap();
        }
        result.setParameters(metadata);

        result.setSd(fromStorageDto(partitionDto.getSerde(), tableName));
        final StorageDescriptor sd = result.getSd();
        if (tableDto != null) {
            if (sd.getSerdeInfo() != null && tableDto.getSerde() != null && Strings.isNullOrEmpty(
                sd.getSerdeInfo().getSerializationLib())) {
                sd.getSerdeInfo().setSerializationLib(tableDto.getSerde().getSerializationLib());
            }

            final List<FieldDto> fields = tableDto.getFields();
            if (fields == null) {
                sd.setCols(Collections.emptyList());
            } else {
                sd.setCols(fields.stream()
                    .filter(field -> !field.isPartition_key())
                    .map(this::metacatToHiveField)
                    .collect(Collectors.toList()));
            }
        }

        final AuditDto auditDto = partitionDto.getAudit();
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
