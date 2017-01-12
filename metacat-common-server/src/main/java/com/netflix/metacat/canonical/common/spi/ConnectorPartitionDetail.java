/*
 *  Copyright 2016 Netflix, Inc.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.canonical.common.spi;

import java.util.Map;

/**
 * Extension of ConnectorPartition.
 */
public interface ConnectorPartitionDetail {
    /**
     * Gets any extra properties of a partition that is relevant to a particular catalog.
     * @return extra properties other than the partition key
     */
    Map<String, String> getMetadata();

    /**
     * Gets the storage related information about the partition. This applies mostly in the case of unstructured
     * data stored as files.
     * @return storage information related properties
     */
    StorageInfo getStorageInfo();

    /**
     * Gets the audit information like created date, last update date etc..
     * @return audit information
     */
    AuditInfo getAuditInfo();
    /**
     * Get the unique id of this partition within the scope of the table.
     * @return string
     */
    String getPartitionId();

}
