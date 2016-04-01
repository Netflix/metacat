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

package com.facebook.presto.exception;

import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;

/**
 * Created by amajumdar on 4/30/15.
 */
public class PartitionNotFoundException extends NotFoundException{
    private final SchemaTableName tableName;
    private final String partitionId;
    public PartitionNotFoundException(SchemaTableName tableName, String partitionId) {
        this(tableName, partitionId, null);
    }

    public PartitionNotFoundException(SchemaTableName tableName, String partitionId, Throwable cause) {
        this(tableName, partitionId,
                String.format("Partition %s not found for table %s", partitionId == null ? "" : partitionId, tableName),
                cause);
    }

    public PartitionNotFoundException(SchemaTableName tableName, String partitionId, String message, Throwable cause) {
        super(message, cause);
        this.tableName = tableName;
        this.partitionId = partitionId;
    }

    public SchemaTableName getTableName() {
        return tableName;
    }

    public String getPartitionId() {
        return partitionId;
    }
}
