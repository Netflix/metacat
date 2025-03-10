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
import java.util.Map;

/**
 * Partition DTO.
 */
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PartitionDto extends BaseDto implements HasDataMetadata, HasDefinitionMetadata {
    private static final long serialVersionUID = 783462697901395508L;
    @Schema(description = "audit information about the partition")
    private AuditDto audit;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @Schema(description = "Physical metadata: metadata about the physical data referred by the partition.")
    @JsonProperty
    private transient ObjectNode dataMetadata;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @Schema(description = "Logical metadata: metadata about the logical construct of the partition.")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    @Schema(description = "the name of this entity", requiredMode = Schema.RequiredMode.REQUIRED)
    @JsonProperty
    private QualifiedName name;
    @Schema(description = "Storage/Serialization/Deserialization info of the partition ")
    private StorageDto serde;
    @Schema(description = "Any extra metadata properties of the partition")
    private Map<String, String> metadata;

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

    @Override
    @JsonProperty
    public boolean isDataExternal() {
        return serde != null && serde.getUri() != null && !serde.getUri().isEmpty();
    }

    /**
     * Sets the data external property.
     *
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
