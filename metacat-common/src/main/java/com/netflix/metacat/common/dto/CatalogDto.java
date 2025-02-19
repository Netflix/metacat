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
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

/**
 * Information about a catalog.
 */
@Schema(description = "Information about a catalog")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
public class CatalogDto extends BaseDto implements HasDefinitionMetadata {
    private static final long serialVersionUID = -5713826608609231492L;

    @Schema(
        description = "a list of the names of the databases that belong to this catalog",
        requiredMode = Schema.RequiredMode.REQUIRED
    )
    private List<String> databases;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @Schema(description = "metadata attached to the logical catalog")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    @Schema(
        description = "the name of this entity",
        requiredMode = Schema.RequiredMode.REQUIRED
    )
    @JsonProperty
    private QualifiedName name;
    @Schema(
        description = "the type of the connector of this catalog",
        requiredMode = Schema.RequiredMode.REQUIRED
    )
    private String type;
    @Schema(
        description = "Cluster information referred to by this catalog",
        requiredMode = Schema.RequiredMode.REQUIRED
    )
    @JsonProperty
    private ClusterDto cluster;
    @JsonProperty
    private Map<String, String> metadata;
    @JsonIgnore
    public QualifiedName getDefinitionName() {
        return name;
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        definitionMetadata = deserializeObjectNode(in);
    }

    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        serializeObjectNode(out, definitionMetadata);
    }
}
