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

package com.netflix.metacat.connector.hive.util;

import lombok.Data;

import javax.annotation.Nonnull;

/**
 * HivePartitionName.
 * @author zhenl
 */
@Data
@Nonnull
public class HivePartitionName {
    private final HiveTableName hiveTableName;
    private final String partitionName;

    /**
     * HivePartitionName.
     * @param databaseName databaseName
     * @param tableName tableName
     * @param partitionName partitionName
     * @return HivePartitionName
     */
    public static HivePartitionName partition(final String databaseName,
                                              final String tableName, final String partitionName) {
        return new HivePartitionName(HiveTableName.table(databaseName, tableName), partitionName);
    }
}
