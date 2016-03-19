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
