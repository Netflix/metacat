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
package com.netflix.metacat.common.server.api.v1;

import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.SortOrder;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Interfaces needed by Thrift.
 * <p>
 * TODO: Get rid of this once Thrift moves to using services.
 *
 * @author tgianos
 * @since 1.1.0
 */
public interface PartitionV1 {

    /**
     * Add/update partitions to the given table.
     *
     * @param catalogName              catalog name
     * @param databaseName             database name
     * @param tableName                table name
     * @param partitionsSaveRequestDto partition request containing the list of partitions to be added/updated
     * @return Response with the number of partitions added/updated
     */
    PartitionsSaveResponseDto savePartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final PartitionsSaveRequestDto partitionsSaveRequestDto
    );

    /**
     * Delete named partitions from a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param partitionIds lis of partition names
     */
    void deletePartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        final List<String> partitionIds
    );

    /**
     * Return list of partitions for a table.
     *
     * @param catalogName             catalog name
     * @param databaseName            database name
     * @param tableName               table name
     * @param sortBy                  sort by this name
     * @param sortOrder               sort order to use
     * @param offset                  offset of the list
     * @param limit                   size of the list
     * @param includeUserMetadata     whether to include user metadata for every partition in the list
     * @param getPartitionsRequestDto request
     * @return list of partitions for a table
     */
    List<PartitionDto> getPartitionsForRequest(
        final String catalogName,
        final String databaseName,
        final String tableName,
        @Nullable final String sortBy,
        @Nullable final SortOrder sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit,
        final boolean includeUserMetadata,
        @Nullable final GetPartitionsRequestDto getPartitionsRequestDto
    );

    /**
     * Return list of partition names for a table.
     *
     * @param catalogName  catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @param filter       filter expression
     * @param sortBy       sort by this name
     * @param sortOrder    sort order to use
     * @param offset       offset of the list
     * @param limit        size of the list
     * @return list of partition names for a table
     */
    List<String> getPartitionKeys(
        final String catalogName,
        final String databaseName,
        final String tableName,
        @Nullable final String filter,
        @Nullable final String sortBy,
        @Nullable final SortOrder sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit
    );

    /**
     * Return list of partitions for a table.
     *
     * @param catalogName         catalog name
     * @param databaseName        database name
     * @param tableName           table name
     * @param filter              filter expression
     * @param sortBy              sort by this name
     * @param sortOrder           sort order to use
     * @param offset              offset of the list
     * @param limit               size of the list
     * @param includeUserMetadata whether to include user metadata for every partition in the list
     * @return list of partitions for a table
     */
    List<PartitionDto> getPartitions(
        final String catalogName,
        final String databaseName,
        final String tableName,
        @Nullable final String filter,
        @Nullable final String sortBy,
        @Nullable final SortOrder sortOrder,
        @Nullable final Integer offset,
        @Nullable final Integer limit,
        final boolean includeUserMetadata
    );
}
