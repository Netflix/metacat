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

import com.netflix.metacat.common.QualifiedName;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Tag Service API.
 *
 * @author amajumdar
 */
public interface TagService {
    /**
     * Returns the list of tags.
     *
     * @return list of tag names
     */
    default Set<String> getTags() {
        return Collections.emptySet();
    }

    /**
     * Returns the list of <code>QualifiedName</code> of items that are tagged by the given <code>includeTags</code> and
     * do not contain the given <code>excludeTags</code>.
     *
     * @param includeTags  include items that contain tags
     * @param excludeTags  include items that do not contain tags
     * @param sourceName   catalog/source name
     * @param databaseName database name
     * @param tableName    table name
     * @return list of qualified names of the items
     */
    default List<QualifiedName> list(
        @Nullable Set<String> includeTags,
        @Nullable Set<String> excludeTags,
        @Nullable String sourceName,
        @Nullable String databaseName,
        @Nullable String tableName
    ) {
        return Collections.emptyList();
    }

    /**
     * Returns the list of <code>QualifiedName</code> of items that have tags containing the given tag text.
     *
     * @param tag          partial text of a tag
     * @param sourceName   source/catalog name
     * @param databaseName database name
     * @param tableName    table name
     * @return list of qualified names of the items
     */
    default List<QualifiedName> search(
        @Nullable String tag,
        @Nullable String sourceName,
        @Nullable String databaseName,
        @Nullable String tableName
    ) {
        return Collections.emptyList();
    }

    /**
     * Tags the given table with the given <code>tags</code>.
     *
     * @param qualifiedName      table name
     * @param tags               list of tags
     * @param updateUserMetadata if true, updates the tags in the user metadata
     * @return return the complete list of tags associated with the table
     */
    default Set<String> setTableTags(
        final QualifiedName qualifiedName,
        final Set<String> tags,
        final boolean updateUserMetadata
    ) {
        return Collections.emptySet();
    }

    /**
     * Removes the tags from the given table.
     *
     * @param qualifiedName      table name
     * @param deleteAll          if true, will delete all tags associated with the given table
     * @param tags               list of tags to be removed for the given table
     * @param updateUserMetadata if true, updates the tags in the user metadata
     */
    default void removeTableTags(
        final QualifiedName qualifiedName,
        final Boolean deleteAll,
        @Nullable final Set<String> tags,
        final boolean updateUserMetadata
    ) {
    }

    /**
     * Delete the tag item along with its associated tags.
     *
     * @param name               table name
     * @param updateUserMetadata if true, updates the tags in the user metadata
     */
    default void delete(final QualifiedName name, final boolean updateUserMetadata) {
    }

    /**
     * Renames the tag item name with the new table name.
     *
     * @param name         table qualified name
     * @param newTableName new table name
     */
    default void rename(final QualifiedName name, final String newTableName) {
    }

    /**
     * Init tag service.
     *
     * @throws Exception error
     */
    default void init() throws Exception {
    }
}
