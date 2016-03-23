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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@ApiModel("Tables and other information about the given database")
@SuppressWarnings("unused")
public class DatabaseDto extends BaseDto implements HasDefinitionMetadata {
    private static final long serialVersionUID = -4530516372664788451L;
    private Date dateCreated;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the logical database")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    private Date lastUpdated;
    @ApiModelProperty(value = "the name of this entity", required = true)
    @JsonProperty
    private QualifiedName name;
    @ApiModelProperty(value = "Names of the tables in this database", required = true)
    private List<String> tables;
    @ApiModelProperty(value = "Connector type of this catalog", required = true)
    private String type;
    @ApiModelProperty(value = "Any extra metadata properties of the database", required = false)
    private Map<String, String> metadata;
    @ApiModelProperty(value = "URI of the database. Only applies to certain data sources like hive, S3", required = false)
    private String uri;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatabaseDto)) return false;
        DatabaseDto that = (DatabaseDto) o;
        return Objects.equals(definitionMetadata, that.definitionMetadata) &&
                Objects.equals(name, that.name) &&
                Objects.equals(tables, that.tables) &&
                Objects.equals(type, that.type) &&
                Objects.equals(lastUpdated, that.lastUpdated) &&
                Objects.equals(dateCreated, that.dateCreated) &&
                Objects.equals(metadata, that.metadata) &&
                Objects.equals(uri, that.uri);
    }

    public Date getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(Date dateCreated) {
        this.dateCreated = dateCreated;
    }

    @Override
    public ObjectNode getDefinitionMetadata() {
        return definitionMetadata;
    }

    @Override
    public void setDefinitionMetadata(ObjectNode metadata) {
        this.definitionMetadata = metadata;
    }

    @JsonIgnore
    public QualifiedName getDefinitionName() {
        return name;
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public QualifiedName getName() {
        return name;
    }

    public void setName(QualifiedName name) {
        this.name = name;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @Override
    public int hashCode() {
        return Objects.hash(definitionMetadata, name, tables, type, lastUpdated, dateCreated, metadata, uri);
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
