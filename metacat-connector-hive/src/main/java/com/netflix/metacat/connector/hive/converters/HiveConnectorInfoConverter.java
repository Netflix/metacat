/*
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
 */

package com.netflix.metacat.connector.hive.converters;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hive connector info converter.
 * @author zhenl
 */
public class HiveConnectorInfoConverter implements ConnectorInfoConverter<Database, Table, Partition> {
    private static final Splitter SLASH_SPLITTER = Splitter.on('/');
    private static final Splitter EQUAL_SPLITTER = Splitter.on('=').limit(2);
    private HiveTypeConverter hiveTypeConverter = new HiveTypeConverter();

    /**
     * Constructor.
      * @param hiveTypeConverter typeconverter
     */
    public HiveConnectorInfoConverter(@Nonnull final HiveTypeConverter hiveTypeConverter) {
        this.hiveTypeConverter = hiveTypeConverter;
    }
    /**
     * Converts to DatabaseDto.
     *
     * @param database connector database
     * @return Metacat database Info
     */
    @Override
    public DatabaseInfo toDatabaseInfo(final QualifiedName qualifiedName, final Database database) {
        return DatabaseInfo.builder().name(qualifiedName).build();
    }

    /**
     * Converts from DatabaseDto to the connector database.
     *
     * @param databaseInfo Metacat database Info
     * @return connector database
     */
    @Override
    public Database fromDatabaseInfo(final DatabaseInfo databaseInfo) {
        final QualifiedName databaseName = databaseInfo.getName();
        final String name = (databaseName == null) ? "" : databaseName.getDatabaseName();
        final String dbUri = Strings.isNullOrEmpty(databaseInfo.getUri()) ? "" : databaseInfo.getUri();
        final Map<String, String> metadata
            = (databaseInfo.getMetadata() != null) ? databaseInfo.getMetadata() : Collections.EMPTY_MAP;
        return new Database(name, name, dbUri, metadata);
    }

    /**
     * Converts to TableDto.
     *
     * @param table connector table
     * @return Metacat table Info
     */
    @Override
    public TableInfo toTableInfo(final QualifiedName name, final Table table) {
        final List<FieldSchema> nonPartitionColumns = table.getSd().getCols();
        final List<FieldSchema> partitionColumns = table.getPartitionKeys();
        final Date creationDate = table.isSetCreateTime() ? epochSecondsToDate(table.getCreateTime()) : null;
        final List<FieldInfo> allFields =
            Lists.newArrayListWithCapacity(nonPartitionColumns.size() + partitionColumns.size());
        nonPartitionColumns.stream()
            .map(field -> hiveToMetacatField(field, false))
            .forEachOrdered(allFields::add);
        partitionColumns.stream()
            .map(field -> hiveToMetacatField(field, true))
            .forEachOrdered(allFields::add);
        final AuditInfo auditInfo = AuditInfo.builder().createdDate(creationDate).build();
        return TableInfo.builder()
            .serde(toStorageInfo(table.getSd(), table.getOwner())).fields(allFields)
            .metadata(table.getParameters()).name(name).auditInfo(auditInfo)
            .build();
    }

    /**
     * Converts from TableDto to the connector table.
     *
     * @param tableInfo Metacat table Info
     * @return connector table
     */
    @Override
    public Table fromTableInfo(final TableInfo tableInfo) {
        final QualifiedName name = tableInfo.getName();
        final String tableName = (name != null) ? name.getTableName() : "";
        final String databaseName = (name != null) ? name.getDatabaseName() : "";

        final StorageInfo storageInfo = tableInfo.getSerde();
        final String owner = (storageInfo != null && storageInfo.getOwner() != null)
            ? storageInfo.getOwner() : "";

        final AuditInfo auditInfo = tableInfo.getAudit();
        final int createTime = (auditInfo != null && auditInfo.getCreatedDate() != null)
            ? dateToEpochSeconds(auditInfo.getCreatedDate()) : 0;

        final Map<String, String> params = (tableInfo.getMetadata() != null)
            ? tableInfo.getMetadata() : Collections.emptyMap();

        final List<FieldInfo> fields = tableInfo.getFields();
        List<FieldSchema> partitionFields = Collections.emptyList();
        List<FieldSchema> nonPartitionFields = Collections.emptyList();
        if (fields != null) {
            nonPartitionFields = Lists.newArrayListWithCapacity(fields.size());
            partitionFields = Lists.newArrayListWithCapacity(fields.size());
            for (FieldInfo fieldInfo : fields) {
                if (fieldInfo.isPartitionKey()) {
                    partitionFields.add(metacatToHiveField(fieldInfo));
                } else {
                    nonPartitionFields.add(metacatToHiveField(fieldInfo));
                }
            }
        }
        final StorageDescriptor sd = fromStorageInfo(storageInfo, nonPartitionFields);

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

    /**
     * Converts to PartitionDto.
     *
     * @param partition connector partition
     * @return Metacat partition Info
     */
    @Override
    public PartitionInfo toPartitionInfo(final TableInfo tableInfo, final Partition partition) {
        final QualifiedName tableName = tableInfo.getName();
        final QualifiedName partitionName = QualifiedName.ofPartition(tableName.getCatalogName(),
            tableName.getDatabaseName(),
            tableName.getTableName(),
            getNameFromPartVals(tableInfo, partition.getValues()));

        final String owner = notNull(tableInfo.getSerde()) ? tableInfo.getSerde().getOwner() : "";
        final AuditInfo auditInfo = AuditInfo.builder()
            .createdDate(epochSecondsToDate(partition.getCreateTime()))
            .lastModifiedDate(epochSecondsToDate(partition.getLastAccessTime())).build();

        return PartitionInfo.builder()
            .serde(toStorageInfo(partition.getSd(), owner))
            .name(partitionName)
            .auditInfo(auditInfo)
            .metadata(partition.getParameters())
            .build();

    }

    /**
     * Converts from PartitionDto to the connector partition.
     *
     * @param partition Metacat partition Info
     * @return connector partition
     */
    @Override
    public Partition fromPartitionInfo(@Nonnull final TableInfo tableInfo, @Nonnull final PartitionInfo partition) {
        final QualifiedName name = partition.getName();
        final List<String> values = Lists.newArrayListWithCapacity(16);
        Map<String, String> metadata = partition.getMetadata();
        if (metadata == null) {
            metadata = Collections.emptyMap();
        }

        final List<FieldInfo> fields = tableInfo.getFields();
        List<FieldSchema> fieldSchemas = Collections.emptyList();
        if (notNull(fields)) {
            fieldSchemas = fields.stream()
                .filter(field -> !field.isPartitionKey())
                .map(this::metacatToHiveField)
                .collect(Collectors.toList());
        }
        final StorageDescriptor sd = fromStorageInfo(partition.getSerde(), fieldSchemas);
        //using the table level seralization lib
        if (notNull(tableInfo) && (notNull(sd.getSerdeInfo())
            && notNull(tableInfo.getSerde()) && Strings.isNullOrEmpty(sd.getSerdeInfo().getSerializationLib()))) {
            sd.getSerdeInfo().setSerializationLib(tableInfo.getSerde().getSerializationLib());
        }
        final AuditInfo auditInfo = partition.getAudit();
        final int createTime = (notNull(auditInfo) && notNull(auditInfo.getCreatedDate()))
            ? dateToEpochSeconds(auditInfo.getCreatedDate()) : 0;
        final int lastAccessTime = (notNull(auditInfo) && notNull(auditInfo.getLastModifiedDate()))
            ? dateToEpochSeconds(auditInfo.getLastModifiedDate()) : 0;

        if (null == name) {
            return new Partition(values, "", "", createTime, lastAccessTime, sd, metadata);
        }

        if (notNull(name.getPartitionName())) {
            for (String partialPartName : SLASH_SPLITTER.split(partition.getName().getPartitionName())) {
                final List<String> nameValues = ImmutableList.copyOf(EQUAL_SPLITTER.split(partialPartName));
                Preconditions.checkState(nameValues.size() == 2,
                    "Unrecognized partition name: " + partition.getName());
                values.add(nameValues.get(1));
            }
        }
        final String databaseName = notNull(name.getDatabaseName()) ? name.getDatabaseName() : "";
        final String tableName = notNull(name.getTableName()) ? name.getTableName() : "";
        return new Partition(
            values,
            databaseName,
            tableName,
            createTime,
            lastAccessTime,
            sd,
            metadata);
    }

    private FieldSchema metacatToHiveField(final FieldInfo fieldInfo) {
        final FieldSchema result = new FieldSchema();
        result.setName(fieldInfo.getName());
        result.setType(hiveTypeConverter.fromMetacatType(fieldInfo.getType()));
        result.setComment(fieldInfo.getComment());
        return result;
    }

    private FieldInfo hiveToMetacatField(final FieldSchema field, final boolean isPartitionKey) {
        return FieldInfo.builder().name(field.getName())
            .type(hiveTypeConverter.toMetacatType(field.getType()))
            .sourceType(field.getType())
            .comment(field.getComment())
            .partitionKey(isPartitionKey)
            .build();
    }

    private StorageInfo toStorageInfo(final StorageDescriptor sd, final String owner) {
        if (sd == null) {
            return new StorageInfo();
        }
        if (sd.getSerdeInfo() != null) {
            return StorageInfo.builder().owner(owner)
                .uri(sd.getLocation())
                .inputFormat(sd.getInputFormat())
                .outputFormat(sd.getOutputFormat())
                .parameters(sd.getParameters())
                .serializationLib(sd.getSerdeInfo().getSerializationLib())
                .serdeInfoParameters(sd.getSerdeInfo().getParameters())
                .build();
        }
        return StorageInfo.builder().owner(owner).uri(sd.getLocation()).inputFormat(sd.getInputFormat())
            .outputFormat(sd.getOutputFormat()).parameters(sd.getParameters()).build();
    }

    @VisibleForTesting
    Integer dateToEpochSeconds(final Date date) {
        return null == date ? null : Math.toIntExact(date.toInstant().getEpochSecond());
    }

    private StorageDescriptor fromStorageInfo(final StorageInfo storageInfo, final List<FieldSchema> cols) {
        if (null == storageInfo) {
            return new StorageDescriptor(
                Collections.emptyList(),
                "",
                "",
                "",
                false,
                0,
                new SerDeInfo("", "", Collections.emptyMap()),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap());
        }
        // Set all required fields to a non-null value
        final String inputFormat = notNull(storageInfo.getInputFormat()) ? storageInfo.getInputFormat() : "";
        final String location = notNull(storageInfo.getUri()) ? storageInfo.getUri() : "";
        final String outputFormat = notNull(storageInfo.getOutputFormat()) ? storageInfo.getOutputFormat() : "";
        final Map<String, String> sdParams = notNull(storageInfo.getParameters())
            ? storageInfo.getParameters() : Collections.emptyMap();
        final Map<String, String> serdeParams = notNull(storageInfo.getSerdeInfoParameters())
            ? storageInfo.getSerdeInfoParameters() : Collections.emptyMap();
        final String serializationLib = notNull(storageInfo.getSerializationLib())
            ? storageInfo.getSerializationLib() : "";
        return new StorageDescriptor(
            cols,
            location,
            inputFormat,
            outputFormat,
            false,
            0,
            new SerDeInfo("", serializationLib, serdeParams),
            Collections.emptyList(),
            Collections.emptyList(),
            sdParams);
    }

    static Date epochSecondsToDate(final long seconds) {
        return Date.from(Instant.ofEpochSecond(seconds));
    }

    private String getNameFromPartVals(final TableInfo tableInfo, final List<String> partVals) {
        final List<String> partitionKeys = getPartitionKeys(tableInfo.getFields());
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

    private List<String> getPartitionKeys(final List<FieldInfo> fieldInfos) {
        if (fieldInfos == null) {
            return null;
        } else if (fieldInfos.isEmpty()) {
            return Collections.emptyList();
        }

        final List<String> keys = new LinkedList<>();
        for (FieldInfo field : fieldInfos) {
            if (field.isPartitionKey()) {
                keys.add(field.getName());
            }
        }
        return keys;
    }

    private boolean notNull(final Object object) {
        return null != object;
    }
}
