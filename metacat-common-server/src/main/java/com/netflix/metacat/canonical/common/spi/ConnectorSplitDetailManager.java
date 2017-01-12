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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.metacat.canonical.common.spi.util.Pageable;
import com.netflix.metacat.canonical.common.spi.util.SavePartitionResult;
import com.netflix.metacat.canonical.common.spi.util.Sort;

import java.util.List;
import java.util.Map;

/**
 * Extension of ConnectorSplitManager providing more helpful methods.
 */
public interface ConnectorSplitDetailManager {
    /**
     * Gets the Partitions based on a filter expression for the specified table.
     * @param table table handle
     * @param filterExpression JSP based filter expression string
     * @param partitionNames filter the list that matches the given partition names.
     *                       If null or empty, it will return all.
     * @param sort sort by and order
     * @param pageable pagination info
     * @param includePartitionDetails tru if partition details need to be included
     * @return filtered list of partitions
     */
    ConnectorPartitionResult getPartitions(ConnectorTableHandle table, String filterExpression,
                                           List<String> partitionNames, Sort sort, Pageable pageable, boolean includePartitionDetails);

    /**
     * Add/Update/delete partitions for a table.
     * @param table table handle
     * @param partitions list of partitions
     * @param partitionIdsForDeletes list of partition ids/names for deletes
     * @param checkIfExists check if partition already exists
     * @param alterIfExists if exists, alter the partition instead of dropping and recreating
     * @return added/updated list of partition names
     */
    SavePartitionResult savePartitions(ConnectorTableHandle table, List<ConnectorPartition> partitions,
                                       List<String> partitionIdsForDeletes,
                                       boolean checkIfExists, boolean alterIfExists);

    /**
     * Delete partitions for a table.
     * @param table table handle
     * @param partitionIds list of partition names
     */
    void deletePartitions(ConnectorTableHandle table, List<String> partitionIds);

    /**
     * Number of partitions for the given table.
     * @param connectorHandle table handle
     * @return Number of partitions
     */
    Integer getPartitionCount(ConnectorTableHandle connectorHandle);

    /**
     * Returns all the partition names referring to the given <code>uris</code>.
     * @param uris locations
     * @param prefixSearch if tru, we look for tables whose location starts with the given <code>uri</code>
     * @return list of partition names
     */
    default Map<String, List<SchemaTablePartitionName>> getPartitionNames(List<String> uris, boolean prefixSearch) {
        return Maps.newHashMap();
    }

    /**
     * Gets the partition names/keys based on a filter expression for the specified table.
     * @param table table handle
     * @param filterExpression JSP based filter expression string
     * @param partitionNames filter the list that matches the given partition names.
     *                       If null or empty, it will return all.
     * @param sort sort by and order
     * @param pageable pagination info
     * @return filtered list of partition names
     */
    default List<String> getPartitionKeys(ConnectorTableHandle table, String filterExpression,
                                          List<String> partitionNames, Sort sort, Pageable pageable) {
        return Lists.newArrayList();
    }

    /**
     * Gets the partition uris based on a filter expression for the specified table.
     * @param table table handle
     * @param filterExpression JSP based filter expression string
     * @param partitionNames filter the list that matches the given partition names.
     *                       If null or empty, it will return all.
     * @param sort sort by and order
     * @param pageable pagination info
     * @return filtered list of partition uris
     */
    default List<String> getPartitionUris(ConnectorTableHandle table, String filterExpression,
                                          List<String> partitionNames, Sort sort, Pageable pageable) {
        return Lists.newArrayList();
    }
}
