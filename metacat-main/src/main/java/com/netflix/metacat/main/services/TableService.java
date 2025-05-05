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
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.TableDto;

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
    TableDto deleteAndReturn(QualifiedName name, boolean isMView);

    /**
     * Returns the table with the given name.
     * @param name qualified name of the table
     * @param getTableServiceParameters  get table parameters
     * @return Returns the table with the given name
     */
    Optional<TableDto> get(QualifiedName name, GetTableServiceParameters getTableServiceParameters);

    /**
     * Rename the table from <code>oldName</code> to <code>newName</code>.
     * @param oldName old qualified name of the existing table
     * @param newName new qualified name of the table
     * @param isMView true, if the object is a view
     */
    void rename(QualifiedName oldName, QualifiedName newName, boolean isMView);

    /**
     * Copies the table metadata from source table <code>name</code> to target table <code>targetName</code>.
     * @param name qualified name of the source table
     * @param targetName qualified name of the target table
     * @return Returns the copied table
     */
    TableDto copy(QualifiedName name, QualifiedName targetName);

    /**
     * Copies the table metadata from source table <code>name</code> to target table <code>targetName</code>.
     * @param tableDto source table
     * @param targetName qualified name of the target table
     * @return Returns the copied table
     */
    TableDto copy(TableDto tableDto, QualifiedName targetName);

    /**
     * Saves the user metadata for the given table.
     * @param name qualified name of the table
     * @param definitionMetadata user definition metadata json
     * @param dataMetadata user data metadata json
     */
    void saveMetadata(QualifiedName name, ObjectNode definitionMetadata, ObjectNode dataMetadata);

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

    /**
     * Returns a list of qualified names of tables that matches the given filter.
     * @param name          catalog name
     * @param parameters    parameters used to get the table names
     * @return list of table names
     */
    List<QualifiedName> getQualifiedNames(QualifiedName name, GetTableNamesServiceParameters parameters);

    /**
     * Updates the object and return the updated object.
     *
     * @param name qualified name of the object
     * @param dto  object dto
     * @param shouldThrowExceptionOnMetadataSaveFailure shouldThrowExceptionOnMetadataSaveFailure
     * @return updated object
     */
    TableDto updateAndReturn(QualifiedName name, TableDto dto, boolean shouldThrowExceptionOnMetadataSaveFailure);
}
