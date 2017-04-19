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

package com.netflix.metacat.common.server.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.HasMetadata;

import javax.annotation.Nonnull;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Usermetadata service API.
 * @author amajumdar
 */
public interface UserMetadataService {
    /** Config location. */
    String METACAT_USERMETADATA_CONFIG_LOCATION = "metacat.usermetadata.config.location";

    /**
     * Delete data metadata for the given uris.
     * @param uris list of uris.
     */
    void deleteDataMetadatas(
        @Nonnull
            List<String> uris);

    /**
     * Delete the delete markers for data metadata for the given uris.
     * @param uris list of uris.
     */
    void deleteDataMetadataDeletes(
        @Nonnull
            List<String> uris);

    /**
     * Mark data metadatas for the given uris for deletion.
     * @param userId user name
     * @param uris list of uris
     */
    void softDeleteDataMetadatas(String userId,
        @Nonnull
            List<String> uris);

    /**
     * Delete definition metadatas for the given names.
     * @param names list of names
     */
    void deleteDefinitionMetadatas(
        @Nonnull
            List<QualifiedName> names);

    /**
     * Delete definition metadata and soft delete data metadata.
     * @param userId username
     * @param holders metadatas
     */
    void deleteMetadatas(String userId, List<HasMetadata> holders);

    /**
     * Returns data metadata for the given uri.
     * @param uri uri.
     * @return data metadata for the given uri.
     */
    @Nonnull
    Optional<ObjectNode> getDataMetadata(
        @Nonnull
            String uri);

    /**
     * Returns the map of uri to data metadata.
     * @param uris list of uris.
     * @return map of uri to data metadata.
     */
    @Nonnull
    Map<String, ObjectNode> getDataMetadataMap(
        @Nonnull
            List<String> uris);

    /**
     * Returns the definition metadata for the given name.
     * @param name name
     * @return definition metadata for the given name
     */
    @Nonnull
    Optional<ObjectNode> getDefinitionMetadata(
        @Nonnull
            QualifiedName name);

    /**
     * Returns the descendants for the given name.
     * @param name name
     * @return list of qualified names
     */
    List<QualifiedName> getDescendantDefinitionNames(
        @Nonnull
            QualifiedName name);

    /**
     * Returns the descendant uris.
     * @param uri uri
     * @return list of descendant uris.
     */
    List<String> getDescendantDataUris(
        @Nonnull
            String uri);

    /**
     * Returns a map of name to definition metadata.
     * @param names list of names
     * @return map of name to definition metadata
     */
    @Nonnull
    Map<String, ObjectNode> getDefinitionMetadataMap(
        @Nonnull
            List<QualifiedName> names);

    /**
     * Save data metadata.
     * @param uri uri
     * @param userId user name
     * @param metadata metadata
     * @param merge if true, will merge with existing metadata
     */
    void saveDataMetadata(
        @Nonnull
            String uri,
        @Nonnull
            String userId,
        @Nonnull
            Optional<ObjectNode> metadata, boolean merge);

    /**
     * Saves definition metadata.
     * @param name name
     * @param userId username
     * @param metadata metadata
     * @param merge if true, will merge with existing metadata
     */
    void saveDefinitionMetadata(
        @Nonnull
            QualifiedName name,
        @Nonnull
            String userId,
        @Nonnull
            Optional<ObjectNode> metadata, boolean merge);

    /**
     * Save metadata.
     * @param userId username
     * @param holder metadata
     * @param merge if true, will merge with existing metadata
     */
    void saveMetadata(String userId, HasMetadata holder, boolean merge);

    /**
     * Populate the metadata.
     * @param holder metadata
     */
    void populateMetadata(HasMetadata holder);

    /**
     * Populate the metadata.
     * @param holder metadata
     * @param definitionMetadata definition metadata
     * @param dataMetadata data metadata
     */
    void populateMetadata(HasMetadata holder, ObjectNode definitionMetadata, ObjectNode dataMetadata);

    /**
     * Rename data metadata uri.
     * @param oldUri old uri
     * @param newUri new uri
     * @return number of records updated
     */
    int renameDataMetadataKey(
        @Nonnull
            String oldUri,
        @Nonnull
            String newUri);

    /**
     * Rename definition metadata name.
     * @param oldName old name
     * @param newName new name
     * @return number of records updated
     */
    int renameDefinitionMetadataKey(
        @Nonnull
            QualifiedName oldName,
        @Nonnull
            QualifiedName newName);

    /**
     * Start the user metadata service.
     * @throws Exception error
     */
    void start() throws Exception;

    /**
     * Stop the user metadata service.
     * @throws Exception error
     */
    void stop() throws Exception;

    /**
     * Saves metadata.
     * @param user username
     * @param holders metadatas
     * @param merge if true, will merge with existing metadata
     */
    void saveMetadatas(String user, List<? extends HasMetadata> holders, boolean merge);

    /**
     * Return the list of definition metadata for the given property names.
     * @param propertyNames names
     * @param type type
     * @param name name
     * @param sortBy sort column
     * @param sortOrder sort order
     * @param offset offset
     * @param limit size of the list
     * @return list of definition metadata
     */
    List<DefinitionMetadataDto> searchDefinitionMetadatas(Set<String> propertyNames, String type, String name,
        String sortBy, String sortOrder, Integer offset, Integer limit);

    /**
     * List the names for the given owners.
     * @param owners list of owner names.
     * @return list of qualified names
     */
    List<QualifiedName> searchByOwners(Set<String> owners);

    /**
     * List of uris marked for deletion.
     * @param deletedPriorTo date
     * @param offset offset
     * @param limit size of the list
     * @return list of uris.
     */
    List<String> getDeletedDataMetadataUris(Date deletedPriorTo, Integer offset, Integer limit);
}
