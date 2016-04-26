/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

/**
 * Information required to create a new catalog
 */
@ApiModel("Information required to create a new catalog")
public class CreateCatalogDto extends BaseDto implements HasDefinitionMetadata {
    private static final long serialVersionUID = -6745573078608938941L;

    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the logical catalog")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    private QualifiedName name;
    @ApiModelProperty(value = "the type of the connector of this catalog", required = true)
    private String type;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CreateCatalogDto))
            return false;
        CreateCatalogDto that = (CreateCatalogDto) o;
        return Objects.equals(definitionMetadata, that.definitionMetadata) &&
                Objects.equals(name, that.name) &&
                Objects.equals(type, that.type);
    }

    @Override
    public ObjectNode getDefinitionMetadata() {
        return definitionMetadata;
    }

    @Override
    public void setDefinitionMetadata(ObjectNode metadata) {
        this.definitionMetadata = metadata;
    }

    @Override
    @JsonIgnore
    public QualifiedName getDefinitionName() {
        return name;
    }

    @ApiModelProperty(value = "the name of this entity", required = true)
    @JsonProperty
    public QualifiedName getName() {
        return name;
    }

    public void setName(QualifiedName name) {
        this.name = name;
    }

    /**
     * @return the name of the connector
     */
    public String getType() {
        return type;
    }

    /**
     * @param type the name of the connector used by this catalog
     */
    public void setType(String type) {
        this.type = type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(definitionMetadata, name, type);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        definitionMetadata = deserializeObjectNode(in);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        serializeObjectNode(out, definitionMetadata);
    }
}
