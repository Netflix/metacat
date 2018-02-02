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

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.GetPartitionsRequestDto;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveRequestDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;
import com.netflix.metacat.common.dto.Sort;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Partition service.
 */
public interface PartitionService extends MetacatService<PartitionDto> {
    /**
     * Returns the list of partitions.
     *
     * @param name                          table name
     * @param sort                          sort info
     * @param pageable                      pagination info
     * @param includeUserDefinitionMetadata if true, includes the definition metadata
     * @param includeUserDataMetadata       if true, includes the data metadata
     * @param getPartitionsRequestDto       getPartitionsRequestDto
     * @return list of partitions
     */
    List<PartitionDto> list(
        QualifiedName name,
        @Nullable Sort sort,
        @Nullable Pageable pageable,
        boolean includeUserDefinitionMetadata,
        boolean includeUserDataMetadata,
        GetPartitionsRequestDto getPartitionsRequestDto);

    /**
     * Partition count for the given table name.
     *
     * @param name table name
     * @return no. of partitions
     */
    Integer count(QualifiedName name);

    /**
     * Saves the list of partitions to the given table <code>name</code>. By default, if a partition exists, it drops
     * the partition before adding it. If <code>alterIfExists</code> is true, then it will alter the partition.
     *
     * @param name                     table name
     * @param partitionsSaveRequestDto request dto containing the partitions to be added and deleted
     * @return no. of partitions added and updated.
     */
    PartitionsSaveResponseDto save(QualifiedName name, PartitionsSaveRequestDto partitionsSaveRequestDto);

    /**
     * Deletes the partitions with the given <code>partitionIds</code> for the given table name.
     *
     * @param name         table name
     * @param partitionIds partition names
     */
    void delete(QualifiedName name, List<String> partitionIds);

    /**
     * Returns the qualified names of partitions that refer to the given uri.
     *
     * @param uri          uri
     * @param prefixSearch if true, this method does a prefix search
     * @return list of names
     */
    List<QualifiedName> getQualifiedNames(String uri, boolean prefixSearch);

    /**
     * Returns a map of uri to qualified names.
     *
     * @param uris         list of uris
     * @param prefixSearch if true, this method does a prefix search
     * @return map of uri to qualified names
     */
    Map<String, List<QualifiedName>> getQualifiedNames(List<String> uris, boolean prefixSearch);

    /**
     * Returns a list of partition names.
     *
     * @param name           table name
     * @param sort           sort info
     * @param pageable       pagination info
     * @param getPartitionsRequestDto get partition request dto
     * @return list of partition names
     */
    List<String> getPartitionKeys(
        QualifiedName name,
        @Nullable Sort sort,
        @Nullable Pageable pageable,
        @Nullable GetPartitionsRequestDto getPartitionsRequestDto);

    /**
     * Returns a list of partition uris.
     *
     * @param name           table name
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
}
