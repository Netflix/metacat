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

package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;

import java.util.List;
import java.util.Objects;

public class MetacatSaveTablePartitionPostEvent extends MetacatEvent {
    private final List<PartitionDto> partitions;
    private final PartitionsSaveResponseDto partitionsSaveResults;

    public MetacatSaveTablePartitionPostEvent(QualifiedName name, List<PartitionDto> partitions,
            PartitionsSaveResponseDto partitionsSaveResults, MetacatContext metacatContext) {
        super(name, metacatContext);
        this.partitions = partitions;
        this.partitionsSaveResults = partitionsSaveResults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof MetacatSaveTablePartitionPostEvent))
            return false;
        if (!super.equals(o))
            return false;
        MetacatSaveTablePartitionPostEvent that = (MetacatSaveTablePartitionPostEvent) o;
        return Objects.equals(partitions, that.partitions) && Objects.equals(partitionsSaveResults,
                that.partitionsSaveResults);
    }

    public List<PartitionDto> getPartitions() {
        return partitions;
    }

    public PartitionsSaveResponseDto getPartitionsSaveResults() {
        return partitionsSaveResults;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(partitions) + Objects.hash(partitionsSaveResults);
    }

    @Override
    public String toString() {
        return "MetacatSaveTablePartitionPostEvent{" +
                "partitions=" + partitions +
                "partitionsSaveResults=" + partitionsSaveResults +
                '}';
    }
}
