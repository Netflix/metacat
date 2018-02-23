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
package com.netflix.metacat.common.dto.notifications.sns;

/**
 * Enumeration of the various types of SNS events there can be.
 *
 * @author tgianos
 * @since 0.1.47
 */
public enum SNSMessageType {
    /**
     * When a table is created.
     */
    TABLE_CREATE,

    /**
     * When a table is deleted.
     */
    TABLE_DELETE,

    /**
     * When the metadata about a table is updated somehow.
     */
    TABLE_UPDATE,

    /**
     * When the partitions for a table are either created or deleted.
     */
    TABLE_PARTITIONS_UPDATE,

    /**
     * When a partition is added.
     */
    PARTITION_ADD,

    /**
     * When a partition is deleted.
     */
    PARTITION_DELETE,

    /**
     * When a partition metadata is saved only.
     */
    PARTITION_METADATAONLY_SAVE
}
