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
package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Table DTO.
 */
@Schema(description = "Table metadata")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableDto extends BaseDto implements HasDataMetadata, HasDefinitionMetadata {
    /** Metadata key holding the Iceberg current snapshot id ({@code -1} if none); absent for non-Iceberg tables. */
    public static final String CURRENT_SNAPSHOT_ID_METADATA_KEY = "current_snapshot_id";
    /** Metadata key holding the JSON array of Iceberg branch names; absent for non-Iceberg tables. */
    public static final String BRANCHES_METADATA_KEY = "branches";
    /** Metadata key holding the Iceberg table format version; absent for non-Iceberg tables. */
    public static final String TABLE_VERSION_METADATA_KEY = "table_version";

    private static final long serialVersionUID = 5922768252406041451L;
    private static final TypeReference<Set<String>> BRANCHES_TYPE_REFERENCE = new TypeReference<Set<String>>() { };

    @Schema(description = "Contains information about table changes")
    private AuditDto audit;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @Schema(description = "metadata attached to the physical data")
    @JsonProperty
    private transient ObjectNode dataMetadata;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @Schema(description = "metadata attached to the logical table")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    private List<FieldDto> fields;
    @Schema(description = "Any extra metadata properties of the database table")
    private Map<String, String> metadata;
    @Schema(description = "the name of this entity", requiredMode = Schema.RequiredMode.REQUIRED)
    @JsonProperty
    private QualifiedName name;
    @Schema(description = "serialization/deserialization info about the table")
    private StorageDto serde;
    @Schema(description = "Hive virtual view info.")
    //Naming as view required by dozer mapping
    private ViewDto view;

    @Nonnull
    @Override
    @JsonIgnore
    public String getDataUri() {
        final String uri = serde != null ? serde.getUri() : null;
        if (uri == null || uri.isEmpty()) {
            throw new IllegalStateException("This instance does not have external data");
        }

        return uri;
    }

    @JsonIgnore
    public QualifiedName getDefinitionName() {
        return name;
    }

    @JsonIgnore
    public Optional<String> getTableOwner() {
        return Optional.ofNullable(definitionMetadata)
                   .map(definitionMetadataJson -> definitionMetadataJson.get("owner"))
                   .map(ownerJson -> ownerJson.get("userId"))
                   .map(JsonNode::textValue);
    }

    @JsonIgnore
    public Optional<String> getTableOwnerGroup() {
        return Optional.ofNullable(definitionMetadata)
                   .map(definitionMetadataJson -> definitionMetadataJson.get("owner"))
                   .map(ownerJson -> ownerJson.get("google_group"))
                   .map(JsonNode::textValue);
    }

    /**
     * Returns the Iceberg current snapshot id ({@code -1} if the table has no snapshot), or empty for a
     * non-Iceberg table or when the {@value #CURRENT_SNAPSHOT_ID_METADATA_KEY} property is missing or unparseable.
     *
     * @return the Iceberg current snapshot id, or empty if unknown
     */
    @JsonIgnore
    public Optional<Long> getCurrentSnapshotId() {
        if (metadata == null) {
            return Optional.empty();
        }
        final String rawSnapshotId = metadata.get(CURRENT_SNAPSHOT_ID_METADATA_KEY);
        if (rawSnapshotId == null || rawSnapshotId.trim().isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Long.parseLong(rawSnapshotId.trim()));
        } catch (final NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * Encodes a set of Iceberg branch names into the string form stored under
     * {@value #BRANCHES_METADATA_KEY} in the metadata map.
     *
     * @param branches the branch names to encode
     * @return the JSON array encoding of the branch names
     */
    public static String encodeBranches(final Collection<String> branches) {
        return METACAT_JSON_LOCATOR.toJsonString(branches);
    }

    /**
     * Returns the set of Iceberg branch names, or empty for a non-Iceberg table or when the
     * {@value #BRANCHES_METADATA_KEY} property is missing or unparseable.
     *
     * @return set of Iceberg branch names, or empty set if unknown
     */
    @JsonIgnore
    public Set<String> getBranches() {
        if (metadata == null) {
            return Collections.emptySet();
        }
        final String rawBranches = metadata.get(BRANCHES_METADATA_KEY);
        if (rawBranches == null || rawBranches.trim().isEmpty()) {
            return Collections.emptySet();
        }
        try {
            return METACAT_JSON_LOCATOR.getObjectMapper().readValue(rawBranches, BRANCHES_TYPE_REFERENCE);
        } catch (final IOException e) {
            return Collections.emptySet();
        }
    }

    /**
     * Returns the Iceberg table format version, or empty for a non-Iceberg table or when the
     * {@value #TABLE_VERSION_METADATA_KEY} property is missing or unparseable.
     *
     * @return the Iceberg table format version, or empty if unknown
     */
    @JsonIgnore
    public Optional<Integer> getTableVersion() {
        if (metadata == null) {
            return Optional.empty();
        }
        final String rawVersion = metadata.get(TABLE_VERSION_METADATA_KEY);
        if (rawVersion == null || rawVersion.trim().isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.parseInt(rawVersion.trim()));
        } catch (final NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * Returns the list of partition keys.
     * @return list of partition keys
     */
    @Schema(description = "List of partition key names")
    @JsonProperty
    @SuppressWarnings("checkstyle:methodname")
    public List<String> getPartition_keys() {
        if (fields == null) {
            return null;
        } else if (fields.isEmpty()) {
            return Collections.emptyList();
        }

        final List<String> keys = new LinkedList<>();
        for (FieldDto field : fields) {
            if (field.isPartition_key()) {
                keys.add(field.getName());
            }
        }
        return keys;
    }

    /**
     * Sets the partition keys.
     * @param ignored list of partition keys
     */
    @SuppressWarnings({"EmptyMethod", "checkstyle:methodname"})
    public void setPartition_keys(final List<String> ignored) {
    }

    @Override
    @JsonProperty
    public boolean isDataExternal() {
        return serde != null && serde.getUri() != null && !serde.getUri().isEmpty();
    }

    /**
     * Sets the data external property.
     * @param ignored is data external
     */
    @SuppressWarnings("EmptyMethod")
    public void setDataExternal(final boolean ignored) {
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        dataMetadata = deserializeObjectNode(in);
        definitionMetadata = deserializeObjectNode(in);
    }

    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        serializeObjectNode(out, dataMetadata);
        serializeObjectNode(out, definitionMetadata);
    }
}
