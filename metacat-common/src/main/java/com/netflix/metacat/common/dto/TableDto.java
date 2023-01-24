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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Table DTO.
 */
@ApiModel(description = "Table metadata")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableDto extends BaseDto implements HasDataMetadata, HasDefinitionMetadata {
    private static final long serialVersionUID = 5922768252406041451L;
    @ApiModelProperty(value = "Contains information about table changes")
    private AuditDto audit;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the physical data")
    @JsonProperty
    private transient ObjectNode dataMetadata;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the logical table")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    private List<FieldDto> fields;
    @ApiModelProperty(value = "Any extra metadata properties of the database table")
    private Map<String, String> metadata;
    @ApiModelProperty(value = "the name of this entity", required = true)
    @JsonProperty
    private QualifiedName name;
    @ApiModelProperty(value = "serialization/deserialization info about the table")
    private StorageDto serde;
    @ApiModelProperty(value = "Hive virtual view info.")
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

    /**
     * Returns the list of partition keys.
     * @return list of partition keys
     */
    @ApiModelProperty(value = "List of partition key names")
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
