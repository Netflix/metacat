package com.netflix.metacat.common.server.usermetadata;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.ChildInfoDto;
import com.netflix.metacat.common.dto.ParentInfoDto;
import com.netflix.metacat.common.server.model.ChildInfo;
import com.netflix.metacat.common.server.model.ParentInfo;
import com.netflix.metacat.common.server.properties.ParentChildRelationshipProperties;
import org.apache.commons.lang3.tuple.Pair;
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
     * 2. Attempting to create a parent table on top of a parent table
     * 3. A child table having more than one parent.
     *
     * @param parentName    the name of the parent entity
     * @param parentUUID    the uuid of the parent
     * @param childName     the name of the child entity
     * @param childUUID     the uuid of the child
     * @param relationType  the type of the relationship
     * @param prop          properties config
     */
    void createParentChildRelation(
        QualifiedName parentName,
        String parentUUID,
        QualifiedName childName,
        String childUUID,
        String relationType,
        ParentChildRelationshipProperties prop
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
     * 1. duplicate all records where the child is `oldName` with childName = `newName`
     * 2. duplicate all records where the parent is `oldName` with parentName `newName`
     *
     * @param oldName  the current name to be renamed
     * @param newName  the new name to rename to
     * @return return a pair of set,
     * where the first set represents the affected parent_uuid with parent = oldName
     * and the second set represents the affected child_uuid with child = oldName
     */
    Pair<Set<String>, Set<String>> renameSoftInsert(
        QualifiedName oldName,
        QualifiedName newName
    );

    /**
     * Removes the entity from the parentChildRelationship store.
     * This involves two steps:
     * 1. drop all records where the child column = `name`
     * 2. drop all records where the parent column = `name`
     * * Note if uuids are specified, it is going to drop the table with that name with the corresponding uuids
     * @param name  the name of the entity to drop
     * @param uuids the uuids to drop, where the first pair is the parent uuid and second pair is the child uuid
     */
    void drop(
        QualifiedName name,
        Optional<Pair<Set<String>, Set<String>>> uuids
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
     * @return a set of ChildInfo dto
     */
    Set<ChildInfoDto> getChildrenDto(
        QualifiedName name
    );

    /**
     * get the set of parent dto for the input name.
     * @param name name
     * @return a set of parentInfo dto
     */
    Set<ParentInfoDto> getParentsDto(QualifiedName name);

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
