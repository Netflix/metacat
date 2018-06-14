/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.main.services.notifications.sns;

/**
 * Enum class for partition add payload message.
 *
 * @author zhenl
 * @since 1.2.0
 */
public enum SNSNotificationPartitionAddMsg {
    /**
     * Attached Valid Paritition Key.
     */
    ATTACHED_VALID_PARITITION_KEY,
    /**
     * Invalid Partition Key Format.
     */
    INVALID_PARTITION_KEY_FORMAT,
    /**
     * All Future Partition Keys.
     */
    ALL_FUTURE_PARTITION_KEYS,
    /**
     * Empty Deleted Column.
     */
    EMPTY_DELETE_COLUMN,
    /**
     * No Candidate Partitions Keys.
     */
    NO_CANDIDATE_PARTITION_KEYS,
    /**
     * Missing Metadata Info For Partition Key.
     */
    MISSING_METADATA_INFO_FOR_PARTITION_KEY,
    /**
     * Failuer of Getting Latest Partition Key.
     */
    FAILURE_OF_GET_LATEST_PARTITION_KEY,
    /**
     * Partition Key Unabled.
     */
    PARTITION_KEY_UNABLED
}
