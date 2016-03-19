package com.netflix.metacat.common.dto;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;

/**
 * Marker interface for objects with data metadata
 */
public interface HasDefinitionMetadata extends HasMetadata {
    ObjectNode getDefinitionMetadata();

    void setDefinitionMetadata(ObjectNode metadata);

    QualifiedName getDefinitionName();
}
