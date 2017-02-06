package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.QualifiedName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

/**
 * Base class for catalog resources.
 * @author amajumdar
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public abstract class BaseInfo {
    /* Name of the resource */
    private QualifiedName name;
    /* Audit information of the resource */
    private AuditInfo audit;
    /* Metadata properties of the resource */
    private Map<String, String> metadata;
}
