package com.netflix.metacat.main.services;

import com.facebook.presto.metadata.TableHandle;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

public interface TableService {
    /**
     * Creates the table.
     * @param name qualified name of the table
     * @param tableDto table metadata
     */
    void create(@Nonnull QualifiedName name, @Nonnull TableDto tableDto);

    /**
     * Deletes the table. Returns the table metadata of the table deleted.
     * @param name qualified name of the table to be deleted
     * @return
     */
    TableDto delete(@Nonnull QualifiedName name);

    Optional<TableDto> get(@Nonnull QualifiedName name, boolean includeUserMetadata);

    Optional<TableDto> get(@Nonnull QualifiedName name, boolean includeInfo, boolean includeDefinitionMetadata, boolean includeDataMetadata);

    Optional<TableHandle> getTableHandle(@Nonnull QualifiedName name);

    void rename(@Nonnull QualifiedName oldName, @Nonnull QualifiedName newName, boolean isMView);

    void update(@Nonnull QualifiedName name, @Nonnull TableDto tableDto);

    TableDto copy(@Nonnull QualifiedName name, @Nonnull QualifiedName targetName);

    TableDto copy(@Nonnull TableDto tableDto, @Nonnull QualifiedName targetName);

    void saveMetadata(@Nonnull QualifiedName name, ObjectNode definitionMetadata, ObjectNode dataMetadata);

    List<QualifiedName> getQualifiedNames(String uri, boolean prefixSearch);
}
