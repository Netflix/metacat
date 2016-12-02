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

/**
 * Schema table partition name.
 */
public class SchemaTablePartitionName {
    private final SchemaTableName tableName;
    private final String partitionId;

    /**
     * Constructor.
     * @param tableName table name
     * @param partitionId partition name
     */
    public SchemaTablePartitionName(final SchemaTableName tableName, final String partitionId) {
        this.tableName = tableName;
        this.partitionId = partitionId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public SchemaTableName getTableName() {
        return tableName;
    }
}
