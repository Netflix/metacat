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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.NameDateDto;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.dto.TableDto;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * View service.
 */
public interface MViewService extends MetacatService<TableDto> {
    /**
     * Create the view and returns the newly created view.
     *
     * @param name name of the origin table
     * @return view
     */
    TableDto create(QualifiedName name);

    /**
     * Create the view and returns the newly created view.
     *
     * @param name     name of the origin table
     * @param snapshot To snapshot a list of partitions of the table to this view.
     * @param filter   Filter expression string to use
     * @return view
     */
    TableDto createAndSnapshotPartitions(QualifiedName name, boolean snapshot, @Nullable String filter);

    /**
     * Deletes the view and returns the deleted view.
     *
     * @param name name of the view to be deleted
     * @return deleted view
     */
    TableDto deleteAndReturn(QualifiedName name);

    /**
     * Get the view for the given name.
     *
     * @param name name
     * @param parameters getTable parameters
     * @return view
     */
    Optional<TableDto> getOpt(QualifiedName name, GetTableServiceParameters parameters);

    /**
     * Copy partitions from the given table name.
     *
     * @param name   table name
     * @param filter filter
     */
    void snapshotPartitions(QualifiedName name, String filter);

    /**
     * Saves the list of partitions to the given view.
     *
     * @param name                     name
     * @param partitionsSaveRequestDto request dto containing the partitions to be added and deleted
     * @param merge                    if true, this method merges
     * @return no. of partitions added and updated.
     */
    PartitionsSaveResponseDto savePartitions(QualifiedName name,
                                             PartitionsSaveRequestDto partitionsSaveRequestDto, boolean merge);

    /**
     * Deletes the list of partitions with the given ids <code>partitionIds</code>.
     *
     * @param name         view name
     * @param partitionIds partition names
     */
    void deletePartitions(QualifiedName name, List<String> partitionIds);

    /**
     * Returns the list of partitions.
     *
     * @param name                    view name
     * @param sort                    sort info
     * @param pageable                pagination info
     * @param includeUserMetadata     if true, includes the user metadata
     * @param getPartitionsRequestDto  get partitions request
     * @return list of partitions
     */
    List<PartitionDto> listPartitions(
        QualifiedName name,
        @Nullable Sort sort,
        @Nullable Pageable pageable,
        boolean includeUserMetadata,
        @Nullable GetPartitionsRequestDto getPartitionsRequestDto);

    /**
     * Returns a list of partition names.
     *
     * @param name           view name
     * @param sort           sort info
     * @param pageable       pagination info
     * @param getPartitionsRequestDto get partition request dto
     * @return list of partition names
     */
    List<String> getPartitionKeys(
        QualifiedName name,
        @Nullable Sort sort,
        @Nullable Pageable pageable,
        @Nullable GetPartitionsRequestDto getPartitionsRequestDto
        );

    /**
     * Returns a list of partition uris.
     *
     * @param name           view name
     * @param sort           sort info
     * @param pageable       pagination info
     * @param getPartitionsRequestDto get partition request dto
     * @return list of partition uris
     */
    List<String> getPartitionUris(
        QualifiedName name,
        @Nullable Sort sort,
        @Nullable Pageable pageable,
        @Nullable GetPartitionsRequestDto getPartitionsRequestDto);

    /**
     * Partition count for the given view name.
     *
     * @param name view name
     * @return no. of partitions
     */
    Integer partitionCount(QualifiedName name);

    /**
     * Returns the list of view names for the given name.
     *
     * @param qualifiedName name
     * @return list of view names
     */
    List<NameDateDto> list(QualifiedName qualifiedName);

    /**
     * Save metadata for the view.
     *
     * @param name               view name
     * @param definitionMetadata definition metadata
     * @param dataMetadata       data metadata
     */
    void saveMetadata(QualifiedName name, ObjectNode definitionMetadata, ObjectNode dataMetadata);

    /**
     * Rename view.
     *
     * @param name        view name
     * @param newViewName new view name
     */
    void rename(QualifiedName name, QualifiedName newViewName);
}
