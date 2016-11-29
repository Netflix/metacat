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
package com.netflix.metacat.common.server.events;

import com.google.common.collect.Lists;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;

@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetacatSaveTablePartitionPostEvent extends MetacatEvent {

    private final List<PartitionDto> partitions;
    private final PartitionsSaveResponseDto partitionsSaveResponse;

    public MetacatSaveTablePartitionPostEvent(
            @NotNull final QualifiedName name,
            @NotNull final MetacatRequestContext metacatRequestContext,
            @NotNull final List<PartitionDto> partitions,
            @NotNull final PartitionsSaveResponseDto partitionsSaveResponse
    ) {
        super(name, metacatRequestContext);
        this.partitions = Lists.newArrayList(partitions);
        this.partitionsSaveResponse = partitionsSaveResponse;
    }

    /**
     * Get an unmodifiable view of the partitions.
     *
     * @return A list of partitions that can't be modified or it will throw an exception
     */
    public List<PartitionDto> getPartitions() {
        return Collections.unmodifiableList(this.partitions);
    }
}
