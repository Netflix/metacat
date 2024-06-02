package com.netflix.metacat.common.server.usermetadata;
import com.netflix.metacat.common.QualifiedName;
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
     * Create parent child relationship with its relation type.
     *
     * @param parentName  parentName
     * @param childName   childName
     * @param relationType type of the relationship
     */
    void createParentChildRelation(
        QualifiedName parentName,
        QualifiedName childName,
        String relationType
    );

    /**
     * Rename oldName to newName in the parentChildRelationship store.
     *
     * @param oldName  oldName
     * @param newName  newName
     */
    void rename(
        QualifiedName oldName,
        QualifiedName newName
    );

    /**
     * drop the entity from the parentChildRelationship store.
     *
     * @param name  name
     */
    void drop(
        QualifiedName name
    );

    /**
     * get the parent for the input name.
     *
     * @param name  name
     * @return parentInfo
     */
    ParentInfo getParent(
        QualifiedName name
    );

    /**
     * get the set of children for the input name.
     *
     * @param name name
     * @return a set of ChildInfo
     */
    Set<ChildInfo> getChildren(
        QualifiedName name
    );
}
