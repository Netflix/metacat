/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.common.server.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DefinitionMetadataDto;
import com.netflix.metacat.common.dto.HasMetadata;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * User metadata service API.
 *
 * @author amajumdar,zhenl
 */
public interface UserMetadataService {
    /**
     * Config location.
     */
    String METACAT_USERMETADATA_CONFIG_LOCATION = "metacat.usermetadata.config.location";

    /**
     * Datasource key.
     */
    String NAME_DATASOURCE = "metacat-usermetadata";

    /**
     * Delete data metadata for the given uris.
     *
     * @param uris list of uris.
     */
    default void deleteDataMetadata(List<String> uris) {
    }

    /**
     * Delete the delete markers for data metadata for the given uris.
     *
     * @param uris list of uris.
     */
    default void deleteDataMetadataDeletes(List<String> uris) {
    }

    /**
     * Mark data metadatas for the given uris for deletion.
     *
     * @param userId user name
     * @param uris   list of uris
     */
    default void softDeleteDataMetadata(String userId, List<String> uris) {
    }

    /**
     * Delete definition metadatas for the given names.
     *
     * @param names list of names
     */
    default void deleteDefinitionMetadata(List<QualifiedName> names) {
    }

    /**
     * Delete definition metadata and soft delete data metadata.
     *
     * @param userId  username
     * @param holders metadatas
     */
    default void deleteMetadata(String userId, List<HasMetadata> holders) {
    }

    /**
     * Returns data metadata for the given uri.
     *
     * @param uri uri.
     * @return data metadata for the given uri.
     */
    default Optional<ObjectNode> getDataMetadata(String uri) {
        return Optional.empty();
    }

    /**
     * Returns the map of uri to data metadata.
     *
     * @param uris list of uris.
     * @return map of uri to data metadata.
     */
    @Nonnull
    default Map<String, ObjectNode> getDataMetadataMap(List<String> uris) {
        return Collections.emptyMap();
    }

    /**
     * Returns the definition metadata for the given name. This method is used for internal query without
     * applying interceptor.
     *
     * @param name name
     * @return definition metadata for the given name
     */
    default Optional<ObjectNode> getDefinitionMetadata(QualifiedName name) {
        return Optional.empty();
    }

    /**
     * Returns the definition metadata for the given name after applying interceptor. This method should be used for
     * all the calls return to outside. We assume that all the REST apis ( get, update, search, create) will return
     * the same result of a table.
     * @param name                             name
     * @param getMetadataInterceptorParameters get Metadata Interceptor parameters
     * @return definition metadata for the given name
     */
    @Nonnull
    default Optional<ObjectNode> getDefinitionMetadataWithInterceptor(
        QualifiedName name,
        GetMetadataInterceptorParameters getMetadataInterceptorParameters) {
        return Optional.empty();
    }

    /**
     * Returns the descendants for the given name.
     *
     * @param name name
     * @return list of qualified names
     */
    default List<QualifiedName> getDescendantDefinitionNames(QualifiedName name) {
        return Collections.emptyList();
    }

    /**
     * Returns the descendant uris.
     *
     * @param uri uri
     * @return list of descendant uris.
     */
    default List<String> getDescendantDataUris(String uri) {
        return Collections.emptyList();
    }

    /**
     * Returns a map of name to definition metadata. This is used for partitions only. We do not apply interceptor
     * for partition level definition metadata.
     *
     * @param names list of names
     * @return map of name to definition metadata
     */
    @Nonnull
    default Map<String, ObjectNode> getDefinitionMetadataMap(List<QualifiedName> names) {
        return Collections.emptyMap();
    }

    /**
     * Save data metadata.
     *
     * @param uri      uri
     * @param userId   user name
     * @param metadata metadata
     * @param merge    if true, will merge with existing metadata
     */
    default void saveDataMetadata(
        String uri,
        String userId,
        Optional<ObjectNode> metadata,
        boolean merge
    ) {
    }

    /**
     * Saves definition metadata with interceptor applied.
     *
     * @param name     name
     * @param userId   username
     * @param metadata metadata
     * @param merge    if true, will merge with existing metadata
     */
    default void saveDefinitionMetadata(
        QualifiedName name,
        String userId,
        Optional<ObjectNode> metadata,
        boolean merge
    ) {
    }

    /**
     * Save metadata.
     *
     * @param userId username
     * @param holder metadata
     * @param merge  if true, will merge with existing metadata
     */
    default void saveMetadata(String userId, HasMetadata holder, boolean merge) {
    }

    /**
     * Populate the metadata.
     *
     * @param holder metadata
     * @param disableInterceptor diable interceptor
     */
    default void populateMetadata(HasMetadata holder, boolean disableInterceptor) {
    }

    /**
     * Populate the metadata.
     *
     * @param holder             metadata
     * @param definitionMetadata definition metadata
     * @param dataMetadata       data metadata
     */
    default void populateMetadata(HasMetadata holder,
                                  ObjectNode definitionMetadata, @Nullable ObjectNode dataMetadata) {
    }

    /**
     * Rename data metadata uri.
     *
     * @param oldUri old uri
     * @param newUri new uri
     * @return number of records updated
     */
    default int renameDataMetadataKey(String oldUri, String newUri) {
        return 0;
    }

    /**
     * Rename definition metadata name.
     *
     * @param oldName old name
     * @param newName new name
     * @return number of records updated
     */
    default int renameDefinitionMetadataKey(QualifiedName oldName, QualifiedName newName) {
        return 0;
    }

    /**
     * Stop the user metadata service.
     *
     * @throws Exception error
     */
    default void stop() throws Exception {
    }

    /**
     * Saves metadata.
     *
     * @param user    username
     * @param holders metadatas
     * @param merge   if true, will merge with existing metadata
     */
    default void saveMetadata(String user, List<? extends HasMetadata> holders, boolean merge) {
    }

    /**
     * Return the list of definition metadata for the given property names.
     *
     * @param propertyNames names
     * @param type          type
     * @param name          name
     * @param holder        dto
     * @param sortBy        sort column
     * @param sortOrder     sort order
     * @param offset        offset
     * @param limit         size of the list
     * @return list of definition metadata
     */
    default List<DefinitionMetadataDto> searchDefinitionMetadata(
        @Nullable Set<String> propertyNames,
        @Nullable String type,
        @Nullable String name,
        @Nullable HasMetadata holder,
        @Nullable String sortBy,
        @Nullable String sortOrder,
        @Nullable Integer offset,
        @Nullable Integer limit
    ) {
        return Collections.emptyList();
    }


    /**
     * List the names for the given owners.
     *
     * @param owners list of owner names.
     * @return list of qualified names
     */
    default List<QualifiedName> searchByOwners(Set<String> owners) {
        return Collections.emptyList();
    }

    /**
     * List of uris marked for deletion.
     *
     * @param deletedPriorTo date
     * @param offset         offset
     * @param limit          size of the list
     * @return list of uris.
     */
    default List<String> getDeletedDataMetadataUris(Date deletedPriorTo, Integer offset, Integer limit) {
        return Collections.emptyList();
    }
}
