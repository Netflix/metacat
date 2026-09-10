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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.AuditDto;
import com.netflix.metacat.common.dto.FieldDto;
import com.netflix.metacat.common.dto.TableDto;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

class IcebergTableDtoFactoryTest {

    private static final QualifiedName NAME = QualifiedName.ofTable("testhive", "testdb", "testtable");
    private static final String LOCATION = "s3://bucket/iceberg/warehouse/testdb.db/uuid/testtable";
    private static final String METADATA_LOCATION =
        LOCATION + "/metadata/00004-a8a03c37-d255-48c9-a078-c9cea1bc6276.metadata.json";

    private static final Schema SCHEMA = new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get(), "the id"),
        Types.NestedField.optional(2, "dateint", Types.IntegerType.get()),
        Types.NestedField.optional(3, "payload", Types.StringType.get()));

    private static TableMetadata metadata(final PartitionSpec spec, final Map<String, String> properties) {
        return TableMetadata.newTableMetadata(SCHEMA, spec, LOCATION, properties);
    }

    private static TableDto dto(final TableMetadata metadata) {
        return IcebergTableDtoFactory.toTableDto(NAME, METADATA_LOCATION, metadata, null, null);
    }

    @Test
    void nameIsCarriedThrough() {
        assertThat(dto(metadata(PartitionSpec.unpartitioned(), Map.of())).getName().toString())
            .isEqualTo("testhive/testdb/testtable");
    }

    @Test
    void fieldsCarryPositionTypeAndComment() {
        final TableDto dto = dto(metadata(PartitionSpec.unpartitioned(), Map.of()));

        assertThat(dto.getFields())
            .extracting(FieldDto::getName, FieldDto::getType, FieldDto::getPos)
            .containsExactly(
                tuple("id", "bigint", 0),
                tuple("dateint", "int", 1),
                tuple("payload", "string", 2));

        final FieldDto id = dto.getFields().get(0);
        assertThat(id.getComment()).isEqualTo("the id");
        assertThat(id.getIsNullable()).isFalse();
        assertThat(id.getSource_type()).isEqualTo("long");
        assertThat(id.getJsonType().toString()).isEqualTo("\"bigint\"");
        assertThat(dto.getFields().get(1).getIsNullable()).isTrue();
    }

    @Test
    void identityPartitionColumnIsAPartitionKey() {
        final PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("dateint").build();

        assertThat(dto(metadata(spec, Map.of())).getFields())
            .extracting(FieldDto::getName, FieldDto::isPartition_key)
            .containsExactly(tuple("id", false), tuple("dateint", true), tuple("payload", false));
    }

    @Test
    void voidTransformIsNotAPartitionKey() {
        final PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).alwaysNull("dateint").build();

        assertThat(dto(metadata(spec, Map.of())).getFields()).filteredOn(FieldDto::isPartition_key).isEmpty();
    }

    @Test
    void serdeUsesTheTableLocationNotTheMetadataLocation() {
        final TableDto dto = dto(metadata(PartitionSpec.unpartitioned(), Map.of()));

        assertThat(dto.getSerde().getUri()).isEqualTo(LOCATION);
        assertThat(dto.getSerde().getInputFormat()).isEqualTo("org.apache.hadoop.mapred.FileInputFormat");
        assertThat(dto.getSerde().getOutputFormat()).isEqualTo("org.apache.hadoop.mapred.FileOutputFormat");
        assertThat(dto.getSerde().getSerializationLib())
            .isEqualTo("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        assertThat(dto.isDataExternal()).isTrue();
    }

    @Test
    void paramsCarryTheIcebergDerivedEntries() {
        final PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("dateint").build();
        final TableMetadata metadata = metadata(spec, Map.of("owner", "testuser"));

        assertThat(dto(metadata).getMetadata())
            .containsEntry("table_type", "ICEBERG")
            .containsEntry("metadata_location", METADATA_LOCATION)
            .containsEntry("partition_spec", metadata.spec().toString())
            .containsEntry("owner", "testuser")
            .containsEntry("iceberg.has.tags", "false")
            .containsEntry("current_snapshot_id", "-1")
            .containsEntry("branches", "[]")
            .containsEntry("table_version", "2");
    }

    @Test
    void unpartitionedSpecRendersAsAnEmptyList() {
        assertThat(dto(metadata(PartitionSpec.unpartitioned(), Map.of())).getMetadata())
            .containsEntry("partition_spec", "[]");
    }

    @Test
    void derivedParamsCannotBeShadowedByATableProperty() {
        final TableMetadata metadata = metadata(PartitionSpec.unpartitioned(),
            Map.of("current_snapshot_id", "999", "table_version", "99", "table_type", "BOGUS"));

        // table_type is written before the properties, matching Metacat, so that one is overridable
        assertThat(dto(metadata).getMetadata())
            .containsEntry("current_snapshot_id", "-1")
            .containsEntry("table_version", "2")
            .containsEntry("table_type", "BOGUS");
    }

    // Refs are asserted against the helpers rather than a TableMetadata: a ref must point at a
    // snapshot that exists, and Iceberg offers no public way to build one.
    @Test
    void branchesExcludeTags() {
        final Map<String, SnapshotRef> refs = Map.of(
            "main", SnapshotRef.branchBuilder(1L).build(),
            "audit", SnapshotRef.branchBuilder(2L).build(),
            "v1.0", SnapshotRef.tagBuilder(3L).build());

        assertThat(IcebergTableDtoFactory.branches(refs)).containsExactlyInAnyOrder("main", "audit");
        assertThat(IcebergTableDtoFactory.hasTags(refs)).isTrue();
    }

    @Test
    void hasTagsIsFalseWithoutTags() {
        assertThat(IcebergTableDtoFactory.hasTags(Map.of("main", SnapshotRef.branchBuilder(1L).build()))).isFalse();
        assertThat(IcebergTableDtoFactory.hasTags(Map.of())).isFalse();
    }

    @Test
    void tableVersionFollowsTheMetadataFormatVersion() {
        assertThat(dto(metadata(PartitionSpec.unpartitioned(), Map.of("format-version", "1"))).getMetadata())
            .containsEntry("table_version", "1");
    }

    @Test
    void nestedColumnTypesReachTheDtoAsBothStringAndJson() {
        final Schema nested = new Schema(
            Types.NestedField.optional(1, "payload", Types.StructType.of(
                Types.NestedField.optional(2, "Amount", Types.DecimalType.of(30, 2)),
                Types.NestedField.optional(3, "codes",
                    Types.ListType.ofOptional(4, Types.StringType.get())))));
        final TableMetadata metadata =
            TableMetadata.newTableMetadata(nested, PartitionSpec.unpartitioned(), LOCATION, Map.of());

        final FieldDto payload = dto(metadata).getFields().get(0);

        assertThat(payload.getType()).isEqualTo("struct<Amount:decimal(30,2),codes:array<string>>");
        assertThat(payload.getJsonType().toString()).isEqualTo(
            "{\"type\":\"row\",\"fields\":[{\"name\":\"Amount\",\"type\":"
                + "{\"type\":\"decimal\",\"precision\":30,\"scale\":2}},{\"name\":\"codes\",\"type\":"
                + "{\"type\":\"array\",\"elementType\":\"string\"}}]}");
    }

    @Test
    void auditDtoConvertsInstantsAndToleratesNulls() {
        final AuditDto audit = IcebergTableDtoFactory.toAuditDto(
            "creator", Instant.ofEpochSecond(1_700_000_000L), "modifier", null);

        assertThat(audit.getCreatedBy()).isEqualTo("creator");
        assertThat(audit.getCreatedDate()).isEqualTo(Date.from(Instant.ofEpochSecond(1_700_000_000L)));
        assertThat(audit.getLastModifiedBy()).isEqualTo("modifier");
        assertThat(audit.getLastModifiedDate()).isNull();
    }

    @Test
    void definitionMetadataAndAuditArePassedThrough() {
        final ObjectNode definitionMetadata = JsonNodeFactory.instance.objectNode().put("secure", "true");
        final AuditDto audit = new AuditDto();
        audit.setCreatedBy("testuser");
        audit.setCreatedDate(new Date(0L));

        final TableDto dto = IcebergTableDtoFactory.toTableDto(NAME, METADATA_LOCATION,
            metadata(PartitionSpec.unpartitioned(), Map.of()), definitionMetadata, audit);

        assertThat(dto.getDefinitionMetadata()).isSameAs(definitionMetadata);
        assertThat(dto.getAudit().getCreatedBy()).isEqualTo("testuser");
    }
}
