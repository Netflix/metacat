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
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Database information.
 */
@Schema(description = "Tables and other information about the given database")
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = false)
public class DatabaseDto extends BaseDto implements HasDefinitionMetadata {
    private static final long serialVersionUID = -4530516372664788451L;
    private Date dateCreated;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @Schema(description = "metadata attached to the logical database")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    private Date lastUpdated;
    @Schema(description = "the name of this entity", requiredMode = Schema.RequiredMode.REQUIRED)
    @JsonProperty
    private QualifiedName name;
    @Schema(description = "Names of the tables in this database", requiredMode = Schema.RequiredMode.REQUIRED)
    private List<String> tables;
    @Schema(description = "Connector type of this catalog", requiredMode = Schema.RequiredMode.REQUIRED)
    private String type;
    @Schema(description = "Any extra metadata properties of the database")
    private Map<String, String> metadata;
    @Schema(description = "URI of the database. Only applies to certain data sources like hive, S3")
    private String uri;

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
