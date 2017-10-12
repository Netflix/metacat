/*
 *  Copyright 2017 Netflix, Inc.
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
 */
package com.netflix.metacat.connector.hive.sql;

import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import lombok.Data;
import org.apache.hadoop.hive.metastore.api.Partition;

/**
 * A wrapper class to hold the Partition internal ids and the partition either as PartitionInfo or Partition.
 * @author amajumdar
 * @since 1.1.x
 */
@Data
public class PartitionHolder {
    // id of the PARTITIONS table
    private Long id;
    // id of the SDS table
    private Long sdId;
    // id of the SERDES table
    private Long serdeId;
    private PartitionInfo partitionInfo;
    private Partition partition;

    /**
     * Constructor populating the ids and partitionInfo.
     * @param id            partition id
     * @param sdId          partition storage id
     * @param serdeId       partition serde id
     * @param partitionInfo partition info
     */
    public PartitionHolder(final Long id, final Long sdId, final Long serdeId, final PartitionInfo partitionInfo) {
        this.id = id;
        this.sdId = sdId;
        this.serdeId = serdeId;
        this.partitionInfo = partitionInfo;
    }

    /**
     * Constructor populating the partition only.
     * @param partition partition
     */
    public PartitionHolder(final Partition partition) {
        this.partition = partition;
    }
}
