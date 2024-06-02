package com.netflix.metacat.common.server.usermetadata;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.model.ChildInfo;
import com.netflix.metacat.common.server.model.ParentInfo;

import java.util.Optional;
import java.util.Set;

/**
 * Parent-Child Relationship Metadata Service  API.
 *
 * @author yingjianw
 */
public interface ParentChildRelMetadataService {
    /**
     * Establishes a parent-child relationship with a specified relation type.
     * Currently, exceptions are thrown in the following cases:
     * 1. Attempting to create a child table as the parent of another child table.
     * 2. A child table having more than one parent.
     *
     * @param parentName    the name of the parent entity
     * @param parentUUID    the uuid of the parent
     * @param childName     the name of the child entity
     * @param childUUID     the uuid of the child
     * @param relationType  the type of the relationship
     */
    void createParentChildRelation(
        QualifiedName parentName,
        String parentUUID,
        QualifiedName childName,
        String childUUID,
        String relationType
    );

    /**
     * Deletes a parent-child relationship with a specified relation type.
     *
     * @param parentName    the name of the parent entity
     * @param parentUUID    the uuid of the parent
     * @param childName     the name of the child entity
     * @param childUUID     the uuid of the child
     * @param type  the type of the relationship
     */
    void deleteParentChildRelation(
        QualifiedName parentName,
        String parentUUID,
        QualifiedName childName,
        String childUUID,
        String type
    );

    /**
     * Renames `oldName` to `newName` in the parentChildRelationship store.
     * This involves two steps:
     * 1. Rename all records where the child is `oldName` to `newName`
     * and if the uuid is present also include it in the sql search string.
     * 2. Rename all records where the parent is `oldName` to `newName`
     * and if the uuid is present also include it in the sql search string.
     * For now, since a child cannot be a parent of another entity, only one of these actions will need to be performed.
     *
     * @param oldName  the current name to be renamed
     * @param newName  the new name to rename to
     * @param uuid     the uuid of the table
     */
    void rename(
        QualifiedName oldName,
        QualifiedName newName,
        Optional<String> uuid
    );

    /**
     * Removes the entity from the parentChildRelationship store.
     * An exception is thrown if an attempt is made to drop a parent table that still has existing child tables.
     * Note that only dropping a child table will result in the physical deletion of records.
     * When all children of a parent table are dropped, no records remain.
     * Consequently, at then time when dropping the parent table, there should not have any database records
     * with a parent reference to the dropping parent table.
     *
     * @param name  the name of the entity to drop
     * @param uuid  the uuid of the entity
     */
    void drop(
        QualifiedName name,
        Optional<String> uuid
    );

    /**
     * get the set of parent for the input name.
     *
     * @param name  name
     * @param uuid the uuid of the entity
     * @return parentInfo
     */
    Set<ParentInfo> getParents(
        QualifiedName name,
        Optional<String> uuid
    );

    /**
     * get the set of children for the input name.
     *
     * @param name name
     * @param uuid the uuid of the entity
     * @return a set of ChildInfo
     */
    Set<ChildInfo> getChildren(
        QualifiedName name,
        Optional<String> uuid
    );
}
