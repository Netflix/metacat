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
package com.netflix.metacat.common.server.connectors.exception;

import com.netflix.metacat.common.QualifiedName;
import lombok.Getter;

import javax.annotation.Nullable;

/**
 * Exception when partition is not found.
 *
 * @author zhenl
 */
@Getter
public class PartitionNotFoundException extends NotFoundException {
    private final QualifiedName tableName;
    private final String partitionName;

    /**
     * Constructor.
     *
     * @param tableName     table name
     * @param partitionName partition name
     */
    public PartitionNotFoundException(final QualifiedName tableName, final String partitionName) {
        this(tableName, partitionName, null);
    }

    /**
     * Constructor.
     *
     * @param tableName     table name
     * @param partitionName partition name
     * @param cause         error cause
     */
    public PartitionNotFoundException(
        final QualifiedName tableName,
        final String partitionName,
        @Nullable final Throwable cause
    ) {
        super(QualifiedName.ofPartition(tableName.getCatalogName(),
            tableName.getDatabaseName(), tableName.getTableName(), partitionName),
            String.format("Partition %s not found for table %s", partitionName, tableName),
            cause, false, false);
        this.tableName = tableName;
        this.partitionName = partitionName;
    }
}
