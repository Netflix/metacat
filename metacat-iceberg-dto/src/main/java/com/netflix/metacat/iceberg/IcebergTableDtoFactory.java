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
package com.netflix.metacat.iceberg;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.AuditDto;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.StorageDto;
import com.netflix.metacat.common.dto.TableDto;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds the {@link TableDto} Metacat produces for an Iceberg table. Data metadata is not covered:
 * it is keyed by storage uri in Metacat's user metadata store.
 */
public final class IcebergTableDtoFactory {

    private static final String INPUT_FORMAT = "org.apache.hadoop.mapred.FileInputFormat";
    private static final String OUTPUT_FORMAT = "org.apache.hadoop.mapred.FileOutputFormat";
    private static final String SERIALIZATION_LIB = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

    private static final String TABLE_TYPE_PROP = "table_type";
    private static final String ICEBERG_TABLE_TYPE = "ICEBERG";
    private static final String METADATA_LOCATION_PROP = "metadata_location";
    private static final String PARTITION_SPEC_PROP = "partition_spec";
    private static final String HAS_TAGS_PROP = "iceberg.has.tags";
    private static final String CURRENT_SNAPSHOT_ID_PROP = TableDto.CURRENT_SNAPSHOT_ID_METADATA_KEY;
    private static final String BRANCHES_PROP = TableDto.BRANCHES_METADATA_KEY;
    private static final String TABLE_VERSION_PROP = TableDto.TABLE_VERSION_METADATA_KEY;

    private static final String VOID_TRANSFORM = "void";

    private IcebergTableDtoFactory() {
    }

    /**
     * Builds the table DTO.
     *
     * @param name               table name
     * @param metadataLocation   metadata file this DTO describes; unset on metadata built for a
     *                           commit that has not landed yet, hence passed separately
     * @param metadata           only schema, spec, location, properties, refs, currentSnapshot
     *                           and formatVersion are read
     * @param definitionMetadata may be null
     * @param audit              may be null
     * @return the table DTO
     */
    public static TableDto toTableDto(final QualifiedName name,
                                      final String metadataLocation,
                                      final TableMetadata metadata,
                                      final ObjectNode definitionMetadata,
                                      final AuditDto audit) {
        final TableDto dto = new TableDto();
        dto.setName(name);
        dto.setFields(toFieldDtos(metadata));
        dto.setSerde(toSerde(metadata.location()));
        dto.setMetadata(toParams(metadataLocation, metadata));
        dto.setDefinitionMetadata(definitionMetadata);
        dto.setAudit(audit);
        return dto;
    }

    /**
     * Takes values rather than a row type so callers with different persistence models share the
     * {@code Instant} to {@code Date} conversion.
     *
     * @param createdBy        may be null
     * @param createdDate      may be null
     * @param lastModifiedBy   may be null
     * @param lastModifiedDate may be null
     * @return the audit DTO
     */
    public static AuditDto toAuditDto(final String createdBy,
                                      final Instant createdDate,
                                      final String lastModifiedBy,
                                      final Instant lastModifiedDate) {
        final AuditDto audit = new AuditDto();
        audit.setCreatedBy(createdBy);
        audit.setLastModifiedBy(lastModifiedBy);
        if (createdDate != null) {
            audit.setCreatedDate(Date.from(createdDate));
        }
        if (lastModifiedDate != null) {
            audit.setLastModifiedDate(Date.from(lastModifiedDate));
        }
        return audit;
    }

    // Schema columns as field DTOs, flagging partition keys.
    private static List<FieldDto> toFieldDtos(final TableMetadata metadata) {
        final Set<String> partitionColumns = partitionColumns(metadata);
        final List<FieldDto> fields = new ArrayList<>();
        int position = 0;
        for (final Types.NestedField column : metadata.schema().columns()) {
            final FieldDto field = new FieldDto();
            field.setName(column.name());
            field.setType(IcebergTypeConverter.toTypeString(column.type()));
            field.setJsonType(IcebergTypeConverter.toJsonType(column.type()));
            field.setSource_type(column.type().toString());
            field.setIsNullable(column.isOptional());
            field.setComment(column.doc());
            field.setPartition_key(partitionColumns.contains(column.name()));
            field.setPos(position++);
            fields.add(field);
        }
        return fields;
    }

    // A void transform means a later spec unpartitioned the column. Dropped source fields are
    // skipped rather than failing the conversion.
    private static Set<String> partitionColumns(final TableMetadata metadata) {
        final Set<String> names = new LinkedHashSet<>();
        for (final PartitionField field : metadata.spec().fields()) {
            if (field.transform() == null || VOID_TRANSFORM.equalsIgnoreCase(field.transform().toString())) {
                continue;
            }
            final Types.NestedField source = metadata.schema().findField(field.sourceId());
            if (source != null) {
                names.add(source.name());
            }
        }
        return names;
    }

    private static StorageDto toSerde(final String tableLocation) {
        final StorageDto serde = new StorageDto();
        serde.setUri(tableLocation);
        serde.setInputFormat(INPUT_FORMAT);
        serde.setOutputFormat(OUTPUT_FORMAT);
        serde.setSerializationLib(SERIALIZATION_LIB);
        return serde;
    }

    // Order matches Metacat: the first three can be shadowed by a table property of the same
    // name, the last three are written after the properties so they cannot be.
    private static Map<String, String> toParams(final String metadataLocation, final TableMetadata metadata) {
        final Map<String, String> params = new HashMap<>();
        params.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE);
        params.put(METADATA_LOCATION_PROP, metadataLocation);
        params.put(PARTITION_SPEC_PROP, metadata.spec().toString());
        params.putAll(metadata.properties());
        params.put(HAS_TAGS_PROP, String.valueOf(hasTags(metadata.refs())));
        params.put(CURRENT_SNAPSHOT_ID_PROP, String.valueOf(
            metadata.currentSnapshot() == null ? -1L : metadata.currentSnapshot().snapshotId()));
        params.put(BRANCHES_PROP, TableDto.encodeBranches(branches(metadata.refs())));
        params.put(TABLE_VERSION_PROP, String.valueOf(metadata.formatVersion()));
        return params;
    }

    static Collection<String> branches(final Map<String, SnapshotRef> refs) {
        final Set<String> branches = new LinkedHashSet<>();
        refs.forEach((refName, ref) -> {
            if (ref.isBranch()) {
                branches.add(refName);
            }
        });
        return branches;
    }

    static boolean hasTags(final Map<String, SnapshotRef> refs) {
        return refs.values().stream().anyMatch(SnapshotRef::isTag);
    }
}
