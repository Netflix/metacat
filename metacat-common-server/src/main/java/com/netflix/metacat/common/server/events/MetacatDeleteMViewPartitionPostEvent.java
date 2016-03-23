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

import java.util.List;
import java.util.Objects;

public class MetacatDeleteMViewPartitionPostEvent extends MetacatEvent {
    private final List<String> partitionIds;

    public MetacatDeleteMViewPartitionPostEvent(QualifiedName name, List<String> partitionIds, MetacatContext metacatContext) {
        super(name, metacatContext);
        this.partitionIds = partitionIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetacatDeleteMViewPartitionPostEvent)) return false;
        if (!super.equals(o)) return false;
        MetacatDeleteMViewPartitionPostEvent that = (MetacatDeleteMViewPartitionPostEvent) o;
        return Objects.equals(partitionIds, that.partitionIds);
    }

    public List<String> getPartitionIds() {
        return partitionIds;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(partitionIds);
    }

    @Override
    public String toString() {
        return "MetacatDeleteMViewPartitionPostEvent{" +
                ", partitions=" + partitionIds +
                '}';
    }
}
