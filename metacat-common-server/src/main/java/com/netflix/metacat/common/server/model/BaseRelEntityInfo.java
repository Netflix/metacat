package com.netflix.metacat.common.server.model;

import lombok.Data;

import java.io.Serializable;

/**
 * Base class to represent relation entity.
 */
@Data
public abstract class BaseRelEntityInfo implements Serializable {
    private static final long serialVersionUID = 9121109874202888889L;
    private String name;
    private String relationType;
    private String uuid;

    /**
     Empty Constructor.
     */
    public BaseRelEntityInfo() {

    }

    /**
     Constructor with all params.
     @param name name of the entity
     @param relationType type of the relation
     @param uuid uuid of the entity
     */
    public BaseRelEntityInfo(final String name, final String relationType, final String uuid) {
        this.name = name;
        this.relationType = relationType;
        this.uuid = uuid;
    }
}
