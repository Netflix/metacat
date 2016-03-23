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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wordnik.swagger.annotations.ApiModelProperty;
import com.wordnik.swagger.annotations.ApiParam;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * Created by amajumdar on 5/4/15.
 */
public class PartitionsSaveRequestDto extends BaseDto{
    private static final long serialVersionUID = -5922699691074685961L;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to this table")
    @JsonProperty
    private transient ObjectNode dataMetadata;
    // Marked as transient because we serialize it manually, however as a JsonProperty because Jackson does serialize it
    @ApiModelProperty(value = "metadata attached to the physical data")
    @JsonProperty
    private transient ObjectNode definitionMetadata;
    // List of partitions
    @ApiParam(value = "List of partitions", required = true)
    private List<PartitionDto> partitions;
    // List of partition ids/names for deletes
    private List<String> partitionIdsForDeletes;
    // If true, we check if partition exists and drop it before adding it back. If false, we do not check and just add.
    private Boolean checkIfExists = true;

    public ObjectNode getDataMetadata() {
        return dataMetadata;
    }

    public void setDataMetadata(ObjectNode dataMetadata) {
        this.dataMetadata = dataMetadata;
    }

    public ObjectNode getDefinitionMetadata() {
        return definitionMetadata;
    }

    public void setDefinitionMetadata(ObjectNode definitionMetadata) {
        this.definitionMetadata = definitionMetadata;
    }

    public List<PartitionDto> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<PartitionDto> partitions) {
        this.partitions = partitions;
    }

    public List<String> getPartitionIdsForDeletes() {
        return partitionIdsForDeletes;
    }

    public void setPartitionIdsForDeletes(List<String> partitionNamesForDeletes) {
        this.partitionIdsForDeletes = partitionNamesForDeletes;
    }

    public Boolean getCheckIfExists() {
        return checkIfExists;
    }

    public void setCheckIfExists(Boolean checkIfExists) {
        this.checkIfExists = checkIfExists;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        dataMetadata = deserializeObjectNode(in);
        definitionMetadata = deserializeObjectNode(in);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        serializeObjectNode(out, dataMetadata);
        serializeObjectNode(out, definitionMetadata);
    }
}
