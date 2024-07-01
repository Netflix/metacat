package com.netflix.metacat.common.server.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * ChildInfo.
 */
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@Data
public class ChildInfo extends BaseRelEntityInfo {
    /**
     Constructor with all params.
     @param name name of the entity
     @param relationType type of the relation
     @param uuid uuid of the entity
     */
    public ChildInfo(final String name, final String relationType, final String uuid) {
        super(name, relationType, uuid);
    }
}
