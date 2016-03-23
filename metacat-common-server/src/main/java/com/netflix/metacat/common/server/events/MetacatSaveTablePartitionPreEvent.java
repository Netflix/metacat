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

import java.util.List;
import java.util.Objects;

public class MetacatSaveTablePartitionPreEvent extends MetacatEvent {
    private final List<PartitionDto> partitions;

    public MetacatSaveTablePartitionPreEvent(QualifiedName name, List<PartitionDto> partitions, MetacatContext metacatContext) {
        super(name, metacatContext);
        this.partitions = partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatSaveTablePartitionPreEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatSaveTablePartitionPreEvent that = (MetacatSaveTablePartitionPreEvent) o;
        return Objects.equals(partitions, that.partitions);
    }

    public List<PartitionDto> getPartitions() {
        return partitions;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(partitions);
    }

    @Override
    public String toString() {
        return "MetacatSaveTablePartitionPreEvent{" +
                "partitions=" + partitions +
                '}';
    }
}
