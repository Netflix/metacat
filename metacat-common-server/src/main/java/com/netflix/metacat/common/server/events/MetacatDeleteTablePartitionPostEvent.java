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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

/**
 * Post table partition delete event.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MetacatDeleteTablePartitionPostEvent extends MetacatEvent {

    private final List<String> partitionIds;

    /**
     * Constructor.
     * @param name name
     * @param requestContext context
     * @param partitionIds partition names
     */
    public MetacatDeleteTablePartitionPostEvent(
        @Nonnull final QualifiedName name,
        @Nonnull final MetacatRequestContext requestContext,
        @Nonnull final List<String> partitionIds
    ) {
        super(name, requestContext);
        this.partitionIds = Collections.unmodifiableList(partitionIds);
    }
}
