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

package com.netflix.metacat.common.server.connectors.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Partition save request.
 * @since 1.0.0
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class PartitionsSaveRequest {
    // List of partitions
    private List<PartitionInfo> partitions;
    // List of partition ids/names for deletes
    private List<String> partitionIdsForDeletes;
    // If true, we check if partition exists and drop it before adding it back. If false, we do not check and just add.
    private Boolean checkIfExists = true;
    // If true, we alter if partition exists. If checkIfExists=false, then this is false too.
    private Boolean alterIfExists = false;
}
