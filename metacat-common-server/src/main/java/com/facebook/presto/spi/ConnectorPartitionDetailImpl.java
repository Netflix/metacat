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

package com.facebook.presto.spi;

import lombok.Data;

import java.util.Map;

/**
 * Connector partition detail.
 */
@Data
public class ConnectorPartitionDetailImpl implements ConnectorPartitionDetail {
    private final String partitionId;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final StorageInfo storageInfo;
    private Map<String, String> metadata;
    private final AuditInfo auditInfo;

    /**
     * Constructor.
     * @param partitionId partition name
     * @param tupleDomain tuple
     * @param metadata metadata
     */
    public ConnectorPartitionDetailImpl(final String partitionId,
        final TupleDomain<ColumnHandle> tupleDomain, final Map<String, String> metadata) {
        this(partitionId, tupleDomain, null, metadata, null);
    }

    /**
     * Constructor.
     * @param partitionId partition name
     * @param tupleDomain tuple
     * @param storageInfo storage info
     * @param metadata metadata
     */
    public ConnectorPartitionDetailImpl(final String partitionId,
        final TupleDomain<ColumnHandle> tupleDomain, final StorageInfo storageInfo,
        final Map<String, String> metadata) {
        this(partitionId, tupleDomain, storageInfo, metadata, null);
    }

    /**
     * Constructor.
     * @param partitionId partition name
     * @param tupleDomain tuple
     * @param storageInfo storage info
     * @param metadata metadata
     * @param auditInfo audit info
     */
    public ConnectorPartitionDetailImpl(final String partitionId,
        final TupleDomain<ColumnHandle> tupleDomain, final StorageInfo storageInfo, final Map<String, String> metadata,
        final AuditInfo auditInfo) {
        this.partitionId = partitionId;
        this.tupleDomain = tupleDomain;
        this.storageInfo = storageInfo;
        this.metadata = metadata;
        this.auditInfo = auditInfo != null ? auditInfo : new AuditInfo();
    }
}
