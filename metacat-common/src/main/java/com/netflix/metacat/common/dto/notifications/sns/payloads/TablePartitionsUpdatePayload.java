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
package com.netflix.metacat.common.dto.notifications.sns.payloads;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Information about how the partitions have changed when a table was updated.
 *
 * @author tgianos
 * @since 0.1.47
 */
@Getter
@ToString
@EqualsAndHashCode
public class TablePartitionsUpdatePayload {
    private final String latestDeleteColumnValue;
    private final int numCreatedPartitions;
    private final int numDeletedPartitions;
    private final String message;
    private final List<String> partitionsUpdated;

    /**
     * Constructor.
     * @param latestDeleteColumnValue The latest DeleteColumn value processed by microbot
     * @param numCreatedPartitions The number of partitions that were created for the table
     * @param numDeletedPartitions The number of partitions that were deleted from the table
     * @param message The message about the partition ids.
     * @param partitionsUpdated The list of ids of the partitions that were updated
     */
    @JsonCreator
    public TablePartitionsUpdatePayload(
        @Nullable @JsonProperty("latestDeleteColumnValue") final String latestDeleteColumnValue,
        @JsonProperty("numCreatedPartitions") final int numCreatedPartitions,
        @JsonProperty("numDeletedPartitions") final int numDeletedPartitions,
        @JsonProperty("message") final String message,
        @JsonProperty("partitionsUpdated") final List<String> partitionsUpdated) {
        this.latestDeleteColumnValue = latestDeleteColumnValue;
        this.numCreatedPartitions = numCreatedPartitions;
        this.numDeletedPartitions = numDeletedPartitions;
        this.message = message;
        this.partitionsUpdated = partitionsUpdated;
    }
}
