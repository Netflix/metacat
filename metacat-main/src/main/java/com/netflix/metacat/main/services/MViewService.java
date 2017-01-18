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

package com.netflix.metacat.main.services;

import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.Sort;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.TableDto;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * View service.
 */
public interface MViewService extends MetacatService<TableDto> {
    /**
     * Create the view and returns the newly created view.
     * @param name name of the origin table
     * @return view
     */
    TableDto create(@Nonnull QualifiedName name);

    /**
     * Create the view and returns the newly created view.
     * @param name name of the origin table
     * @param snapshot To snapshot a list of partitions of the table to this view.
     * @param filter Filter expression string to use
     * @return view
     */
    TableDto createAndSnapshotPartitions(@Nonnull QualifiedName name, boolean snapshot, String filter);

    /**
     * Deletes the view and returns the deleted view.
     * @param name name of the view to be deleted
     * @return deleted view
     */
    TableDto deleteAndReturn(@Nonnull QualifiedName name);

    /**
     * Get the view for the given name.
     * @param name name
     * @return view
     */
    Optional<TableDto> getOpt(@Nonnull QualifiedName name);

    /**
     * Copy partitions from the given table name.
     * @param name table name
     * @param filter filter
     */
    void snapshotPartitions(@Nonnull QualifiedName name, String filter);

    /**
     * Saves the list of partitions to the given view.
     * @param name name
     * @param partitionsSaveRequestDto request dto containing the partitions to be added and deleted
     * @param merge if true, this method merges
     * @return no. of partitions added and updated.
     */
    PartitionsSaveResponseDto savePartitions(@Nonnull QualifiedName name,
        PartitionsSaveRequestDto partitionsSaveRequestDto, boolean merge);

    /**
     * Deletes the list of partitions with the given ids <code>partitionIds</code>.
     * @param name view name
     * @param partitionIds partition names
     */
    void deletePartitions(@Nonnull QualifiedName name, List<String> partitionIds);

    /**
     * Returns the list of partitions.
     * @param name view name
     * @param filter filter expression
     * @param partitionNames partition names to include
     * @param sort sort info
     * @param pageable pagination info
     * @param includeUserMetadata if true, includes the user metadata
     * @param includePartitionDetails if true, includes parameter details
     * @return list of partitions
     */
    List<PartitionDto> listPartitions(@Nonnull QualifiedName name, String filter, List<String> partitionNames,
        Sort sort, Pageable pageable,
        boolean includeUserMetadata, boolean includePartitionDetails);

    /**
     * Returns a list of partition names.
     * @param name view name
     * @param filter filter expression
     * @param partitionNames names
     * @param sort sort info
     * @param pageable pagination info
     * @return list of partition names
     */
    List<String> getPartitionKeys(@Nonnull QualifiedName name, String filter, List<String> partitionNames, Sort sort,
        Pageable pageable);

    /**
     * Returns a list of partition uris.
     * @param name view name
     * @param filter filter expression
     * @param partitionNames names
     * @param sort sort info
     * @param pageable pagination info
     * @return list of partition uris
     */
    List<String> getPartitionUris(@Nonnull QualifiedName name, String filter, List<String> partitionNames, Sort sort,
        Pageable pageable);

    /**
     * Partition count for the given view name.
     * @param name view name
     * @return no. of partitions
     */
    Integer partitionCount(@Nonnull QualifiedName name);

    /**
     * Returns the list of view names for the given name.
     * @param qualifiedName name
     * @return list of view names
     */
    List<NameDateDto> list(@Nonnull QualifiedName qualifiedName);

    /**
     * Save metadata for the view.
     * @param name view name
     * @param definitionMetadata definition metadata
     * @param dataMetadata data metadata
     */
    void saveMetadata(@Nonnull QualifiedName name, ObjectNode definitionMetadata, ObjectNode dataMetadata);

    /**
     * Rename view.
     * @param name view name
     * @param newViewName new view name
     */
    void rename(@Nonnull QualifiedName name, @Nonnull QualifiedName newViewName);
}
