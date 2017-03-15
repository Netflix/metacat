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

import com.netflix.metacat.common.QualifiedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Partition DTO.
 * @since 1.0.0
 */
@SuppressWarnings("unused")
@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public final class PartitionInfo extends BaseInfo {
    private StorageInfo serde;

    /**
     * Constructor.
     * @param name name of the partition
     * @param auditInfo audit information of the partition
     * @param metadata metadata of the partition.
     * @param serde storage info of the partition
     */
    @Builder
    private PartitionInfo(final QualifiedName name, final AuditInfo auditInfo,
                         final Map<String, String> metadata, final StorageInfo serde) {
        super(name, auditInfo, metadata);
        this.serde = serde;
    }
}
