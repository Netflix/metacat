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

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

/**
 * Post table partition save event.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetacatSaveTablePartitionPostEvent extends MetacatEvent {

    private final List<PartitionDto> partitions;
    private final PartitionsSaveResponseDto partitionsSaveResponse;

    /**
     * Constructor.
     * @param name name
     * @param metacatRequestContext context
     * @param partitions partitions
     * @param partitionsSaveResponse resposne
     */
    public MetacatSaveTablePartitionPostEvent(
        @Nonnull final QualifiedName name,
        @Nonnull final MetacatRequestContext metacatRequestContext,
        @Nonnull final List<PartitionDto> partitions,
        @Nonnull final PartitionsSaveResponseDto partitionsSaveResponse
    ) {
        super(name, metacatRequestContext);
        this.partitions = Collections.unmodifiableList(partitions);
        this.partitionsSaveResponse = partitionsSaveResponse;
    }
}
