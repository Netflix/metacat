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

/**
 * Information required to create a new catalog.
 */
@Schema(description = "Information required to create a new catalog")
@Data
@EqualsAndHashCode(callSuper = false)
public class CreateCatalogDto extends BaseDto implements HasDefinitionMetadata {
    private static final long serialVersionUID = -6745573078608938941L;

    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @Schema(description = "metadata attached to the logical catalog")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    @Schema(description = "the name of this entity", requiredMode = Schema.RequiredMode.REQUIRED)
    @JsonProperty
    private QualifiedName name;
    @Schema(description = "the type of the connector of this catalog", requiredMode = Schema.RequiredMode.REQUIRED)
    private String type;

    @Override
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
