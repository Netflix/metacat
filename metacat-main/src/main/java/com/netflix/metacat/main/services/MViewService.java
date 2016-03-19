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

public interface MViewService {
    /**
     * Create the view and returns the newly created view
     * @param name name of the origin table
     * @return view
     */
    TableDto create( @Nonnull QualifiedName name);

    /**
     * Deletes the view and returns the deleted view.
     * @param name name of the view to be deleted
     * @return deleted view
     */
    TableDto delete( @Nonnull QualifiedName name);
    void update(@Nonnull QualifiedName name, @Nonnull TableDto tableDto);
    Optional<TableDto> get( @Nonnull QualifiedName name);
    void snapshotPartitions( @Nonnull QualifiedName name, String filter);
    PartitionsSaveResponseDto savePartitions( @Nonnull QualifiedName name, List<PartitionDto> partitionDtos, List<String> partitionIdsForDeletes, boolean merge, boolean checkIfExists);
    void deletePartitions( @Nonnull QualifiedName name, List<String> partitionIds);
    List<PartitionDto> listPartitions(@Nonnull QualifiedName name, String filter, List<String> partitionNames, Sort sort, Pageable pageable, boolean includeUserMetadata, boolean includePartitionDetails);
    Integer partitionCount(@Nonnull QualifiedName name);
    List<NameDateDto> list(@Nonnull QualifiedName qualifiedName);
    void saveMetadata(@Nonnull QualifiedName name, ObjectNode definitionMetadata, ObjectNode dataMetadata);
    void rename(QualifiedName name, QualifiedName newViewName);
}
