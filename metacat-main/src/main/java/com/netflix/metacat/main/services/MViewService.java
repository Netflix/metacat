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
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.TableDto;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

public interface MViewService extends MetacatService<TableDto> {
    /**
     * Create the view and returns the newly created view
     * @param name name of the origin table
     * @return view
     */
    TableDto create(
        @Nonnull
            QualifiedName name);

    /**
     * Deletes the view and returns the deleted view.
     * @param name name of the view to be deleted
     * @return deleted view
     */
    TableDto deleteAndReturn(
        @Nonnull
            QualifiedName name);

    Optional<TableDto> getOpt(
        @Nonnull
            QualifiedName name);

    void snapshotPartitions(
        @Nonnull
            QualifiedName name, String filter);

    PartitionsSaveResponseDto savePartitions(
        @Nonnull
            QualifiedName name, List<PartitionDto> partitionDtos,
        List<String> partitionIdsForDeletes, boolean merge,
        boolean checkIfExists, boolean alterIfExists);

    void deletePartitions(
        @Nonnull
            QualifiedName name, List<String> partitionIds);

    List<PartitionDto> listPartitions(
        @Nonnull
            QualifiedName name, String filter, List<String> partitionNames,
        Sort sort, Pageable pageable,
        boolean includeUserMetadata, boolean includePartitionDetails);

    List<String> getPartitionKeys(QualifiedName name, String filter, List<String> partitionNames, Sort sort,
        Pageable pageable);

    List<String> getPartitionUris(QualifiedName name, String filter, List<String> partitionNames, Sort sort,
        Pageable pageable);

    Integer partitionCount(
        @Nonnull
            QualifiedName name);

    List<NameDateDto> list(
        @Nonnull
            QualifiedName qualifiedName);

    void saveMetadata(
        @Nonnull
            QualifiedName name, ObjectNode definitionMetadata, ObjectNode dataMetadata);

    void rename(QualifiedName name, QualifiedName newViewName);
}
