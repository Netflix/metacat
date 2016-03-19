package com.netflix.metacat.common.usermetadata;

import com.netflix.metacat.common.QualifiedName;

import java.util.List;
import java.util.Set;

/**
 * Created by amajumdar on 6/29/15.
 */
public interface TagService {
    /**
     * Returns the list of tags
     * @return list of tag names
     */
    Set<String> getTags();

    /**
     * Returns the list of <code>QualifiedName</code> of items that are tagged by the given <code>includeTags</code> and
     * do not contain the given <code>excludeTags</code>
     * @param includeTags include items that contain tags
     * @param excludeTags include items that do not contain tags
     * @param sourceName catalog/source name
     * @param databaseName database name
     * @param tableName table name
     * @return list of qualified names of the items
     */
    List<QualifiedName> list(
            Set<String> includeTags,
            Set<String> excludeTags,
            String sourceName,
            String databaseName,
            String tableName);

    /**
     * Returns the list of <code>QualifiedName</code> of items that have tags containing the given tag text.
     * @param tag partial text of a tag
     * @param sourceName source/catalog name
     * @param databaseName database name
     * @param tableName table name
     * @return list of qualified names of the items
     */
    List<QualifiedName> search(
            String tag,
            String sourceName,
            String databaseName,
            String tableName);

    /**
     * Tags the given table with the given <code>tags</code>
     * @param qualifiedName, table name
     * @param tags list of tags
     * @return return the complete list of tags associated with the table
     */
    Set<String> setTableTags(
            QualifiedName qualifiedName,
            Set<String> tags,
            boolean updateUserMetadata);

    /**
     * Removes the tags from the given table
     * @param qualifiedName table name
     * @param deleteAll if true, will delete all tags associated with the given table
     * @param tags list of tags to be removed for the given table
     */
    Void removeTableTags(
            QualifiedName qualifiedName,
            Boolean deleteAll,
            Set<String> tags,
            boolean updateUserMetadata);

    /**
     * Delete the tag item along with its associated tags.
     * @param name table name
     * @return null
     */
    Void delete(QualifiedName name,
            boolean updateUserMetadata);

    /**
     * Renames the tag item name with the new table name
     * @param name table qualified name
     * @param newTableName new table name
     * @return null
     */
    Void rename(QualifiedName name, String newTableName);
}
