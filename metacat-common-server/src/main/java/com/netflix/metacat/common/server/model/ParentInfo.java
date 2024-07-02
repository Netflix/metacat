package com.netflix.metacat.common.server.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * ParentInfo.
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ParentInfo extends BaseRelEntityInfo {

    /**
     Empty Constructor.
     */
    public ParentInfo() {

    }

    /**
     Constructor with all params.
     @param name name of the entity
     @param relationType type of the relation
     @param uuid uuid of the entity
     */
    public ParentInfo(final String name, final String relationType, final String uuid) {
        super(name, relationType, uuid);
    }
}
