/*
 *
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
 *
 */
package com.netflix.metacat.common.server.connectors;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveRequest;
import com.netflix.metacat.common.server.connectors.model.PartitionsSaveResponse;

import java.util.List;
import java.util.Map;

/**
 * Interfaces for manipulating partition information for this connector.
 *
 * @author tgianos
 * @since 1.0.0
 */
public interface ConnectorPartitionService extends ConnectorBaseService<PartitionInfo> {
    /**
     * Gets the Partitions based on a filter expression for the specified table.
     *
     * @param context           The Metacat request context
     * @param table             table handle to get partition for
     * @param partitionsRequest The metadata for what kind of partitions to get from the table
     * @return filtered list of partitions
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    default List<PartitionInfo> getPartitions(
        final ConnectorContext context,
        final QualifiedName table,
        final PartitionListRequest partitionsRequest
    ) {
        throw new UnsupportedOperationException(ConnectorBaseService.UNSUPPORTED_MESSAGE);
    }

    /**
     * Add/Update/delete partitions for a table.
     *
     * @param context               The Metacat request context
     * @param table                 table handle to get partition for
     * @param partitionsSaveRequest Partitions to save, alter or delete
     * @return added/updated list of partition names
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    default PartitionsSaveResponse savePartitions(
        final ConnectorContext context,
        final QualifiedName table,
        final PartitionsSaveRequest partitionsSaveRequest
    ) {
        throw new UnsupportedOperationException(ConnectorBaseService.UNSUPPORTED_MESSAGE);
    }

    /**
     * Delete partitions for a table.
     *
     * @param context        The Metacat request context
     * @param tableName      table name
     * @param partitionNames list of partition names
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    default void deletePartitions(
        final ConnectorContext context,
        final QualifiedName tableName,
        final List<String> partitionNames
    ) {
        throw new UnsupportedOperationException(ConnectorBaseService.UNSUPPORTED_MESSAGE);
    }

    /**
     * Number of partitions for the given table.
     *
     * @param context The Metacat request context
     * @param table   table handle
     * @return Number of partitions
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    default int getPartitionCount(
        final ConnectorContext context,
        final QualifiedName table
    ) {
        throw new UnsupportedOperationException(ConnectorBaseService.UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns all the partition names referring to the given <code>uris</code>.
     *
     * @param context      The Metacat request context
     * @param uris         locations
     * @param prefixSearch if true, we look for tables whose location starts with the given <code>uri</code>
     * @return map of uri to list of partition names
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    default Map<String, List<QualifiedName>> getPartitionNames(
        final ConnectorContext context,
        final List<String> uris,
        final boolean prefixSearch
    ) {
        throw new UnsupportedOperationException(ConnectorBaseService.UNSUPPORTED_MESSAGE);
    }

    /**
     * Gets the partition names/keys based on a filter expression for the specified table.
     *
     * @param context           The Metacat request context
     * @param table             table handle to get partition for
     * @param partitionsRequest The metadata for what kind of partitions to get from the table
     * @return filtered list of partition names
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    default List<String> getPartitionKeys(
        final ConnectorContext context,
        final QualifiedName table,
        final PartitionListRequest partitionsRequest
    ) {
        throw new UnsupportedOperationException(ConnectorBaseService.UNSUPPORTED_MESSAGE);
    }

    /**
     * Gets the partition uris based on a filter expression for the specified table.
     *
     * @param context           The Metacat request context
     * @param table             table handle to get partition for
     * @param partitionsRequest The metadata for what kind of partitions to get from the table
     * @return filtered list of partition uris
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    default List<String> getPartitionUris(
        final ConnectorContext context,
        final QualifiedName table,
        final PartitionListRequest partitionsRequest
    ) {
        throw new UnsupportedOperationException(ConnectorBaseService.UNSUPPORTED_MESSAGE);
    }
}
