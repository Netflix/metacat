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
import java.util.Map;
import java.util.Optional;

/**
 * Table service.
 */
public interface TableService extends MetacatService<TableDto> {
    /**
     * Deletes the table. Returns the table metadata of the table deleted.
     * @param name qualified name of the table to be deleted
     * @param isMView true if this table is created for a mview
     * @return Returns the deleted table
     */
    TableDto deleteAndReturn(@Nonnull QualifiedName name, boolean isMView);

    /**
     * Returns the table with the given name.
     * @param name qualified name of the table
     * @param includeUserMetadata if true, the table will include the user metadata
     * @return Returns the table with the given name
     */
    Optional<TableDto> get(@Nonnull QualifiedName name, boolean includeUserMetadata);

    /**
     * Returns the table with the given name.
     * @param name qualified name of the table
     * @param includeInfo if true, the table will include the main table metadata
     * @param includeDefinitionMetadata if true, the table will include the user definition metadata
     * @param includeDataMetadata if true, the table will include the user data metadata
     * @return Returns the table with the given name
     */
    Optional<TableDto> get(@Nonnull QualifiedName name, boolean includeInfo,
        boolean includeDefinitionMetadata, boolean includeDataMetadata);

    /**
     * Returns the table handle.
     * @param name qualified name of the table
     * @return Returns the table handle with the given name
     */
    Optional<TableHandle> getTableHandle(@Nonnull QualifiedName name);

    /**
     * Rename the table from <code>oldName</code> to <code>newName</code>.
     * @param oldName old qualified name of the existing table
     * @param newName new qualified name of the table
     * @param isMView true, if the object is a view
     */
    void rename(@Nonnull QualifiedName oldName, @Nonnull QualifiedName newName, boolean isMView);

    /**
     * Copies the table metadata from source table <code>name</code> to target table <code>targetName</code>.
     * @param name qualified name of the source table
     * @param targetName qualified name of the target table
     * @return Returns the copied table
     */
    TableDto copy(@Nonnull QualifiedName name, @Nonnull QualifiedName targetName);

    /**
     * Copies the table metadata from source table <code>name</code> to target table <code>targetName</code>.
     * @param tableDto source table
     * @param targetName qualified name of the target table
     * @return Returns the copied table
     */
    TableDto copy(@Nonnull TableDto tableDto, @Nonnull QualifiedName targetName);

    /**
     * Saves the user metadata for the given table.
     * @param name qualified name of the table
     * @param definitionMetadata user definition metadata json
     * @param dataMetadata user data metadata json
     */
    void saveMetadata(@Nonnull QualifiedName name, ObjectNode definitionMetadata, ObjectNode dataMetadata);

    /**
     * Returns a list of qualified names of tables that refers to the given <code>uri</code>. If prefixSearch is true,
     * it will consider the uri has a prefix and so it does not do a exact match.
     * @param uri uri/location
     * @param prefixSearch if false, the method looks for exact match for the uri
     * @return list of table names
     */
    List<QualifiedName> getQualifiedNames(String uri, boolean prefixSearch);

    /**
     * Returns a map of list of qualified names of tables that refers to the given <code>uri</code>.
     * If prefixSearch is true, it will consider the uri has a prefix and so it does not do a exact match.
     * @param uris uris/locations
     * @param prefixSearch if false, the method looks for exact match for the uri
     * @return Map of list of table names
     */
    Map<String, List<QualifiedName>> getQualifiedNames(List<String> uris, boolean prefixSearch);
}
