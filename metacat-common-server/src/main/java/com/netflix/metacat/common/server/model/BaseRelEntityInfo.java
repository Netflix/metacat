package com.netflix.metacat.common.server.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Base class to represent relation entity.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class BaseRelEntityInfo implements Serializable {
    private static final long serialVersionUID = 9121109874202888889L;
    private String name;
    private String relationType;
    private String uuid;

}
