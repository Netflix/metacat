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
