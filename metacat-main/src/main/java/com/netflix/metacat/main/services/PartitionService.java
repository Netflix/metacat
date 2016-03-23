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
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.PartitionsSaveResponseDto;

import java.util.List;

public interface PartitionService {
    List<PartitionDto> list(QualifiedName name, String filter, List<String> partitionNames, Sort sort, Pageable pageable, boolean includeUserDefinitionMetadata, boolean includeUserDataMetadata, boolean includePartitionDetails);
    Integer count(QualifiedName name);
    PartitionsSaveResponseDto save(QualifiedName name, List<PartitionDto> partitionDtos, List<String> partitionIdsForDeletes, boolean checkIfExists);
    void delete(QualifiedName name, List<String> partitionIds);
    List<QualifiedName> getQualifiedNames(String uri, boolean prefixSearch);
}
