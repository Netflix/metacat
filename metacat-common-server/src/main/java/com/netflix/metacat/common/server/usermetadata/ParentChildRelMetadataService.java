package com.netflix.metacat.common.server.usermetadata;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.notifications.ChildInfoDto;
import com.netflix.metacat.common.server.model.ChildInfo;
import com.netflix.metacat.common.server.model.ParentInfo;

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
     * 2. Attempting to create a parent table on top of a parent table
     * 3. A child table having more than one parent.
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
     * This function is only called in the recovery process when
     * we first create the parent-child relationship but fail to create the table.
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
     * 2. Rename all records where the parent is `oldName` to `newName`
     *
     * @param oldName  the current name to be renamed
     * @param newName  the new name to rename to
     */
    void rename(
        QualifiedName oldName,
        QualifiedName newName
    );

    /**
     * Removes the entity from the parentChildRelationship store.
     * This involves two steps:
     * 1. drop all records where the child column = `name`
     * 2. drop all records where the parent column = `name`
     * @param name  the name of the entity to drop
     */
    void drop(
        QualifiedName name
    );

    /**
     * get the set of parent for the input name.
     * @param name  name
     * @return parentInfo
     */
    Set<ParentInfo> getParents(
        QualifiedName name
    );

    /**
     * get the set of children for the input name.
     * @param name name
     * @return a set of ChildInfo
     */
    Set<ChildInfo> getChildren(
        QualifiedName name
    );

    /**
     * get the set of children dto for the input name.
     * @param name name
     * @return a set of ChildInfo
     */
    Set<ChildInfoDto> getChildrenDto(
        QualifiedName name
    );

    /**
     * return whether the table is a parent.
     * @param tableName tableName
     * @return true if it exists
     */
    boolean isParentTable(final QualifiedName tableName);

    /**
     * return whether the table is a child.
     * @param tableName tableName
     * @return true if it exists
     */
    boolean isChildTable(final QualifiedName tableName);
}
