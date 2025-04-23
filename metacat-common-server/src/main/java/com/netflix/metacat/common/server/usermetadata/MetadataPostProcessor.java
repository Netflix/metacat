package com.netflix.metacat.common.server.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;

/**
 * MetadataPostProcessor: This processor runs after storing the definitionMetadata.
 * @author yingjianw
 * @since 1.2.0
 */
public interface MetadataPostProcessor {
    /**
     * postProcess the newMetadata.
     * @param userMetadataService user metadata service
     * @param name                qualified name
     * @param newMetadata         newMetadata
     */
    default void postProcess(
        UserMetadataService userMetadataService,
        QualifiedName name,
        ObjectNode newMetadata) {

    }
}
