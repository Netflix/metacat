package com.netflix.metacat.common.server.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetadataException;

import java.util.Optional;

/**
 * MetadataPreMergeInterceptor: This interceptor runs before merging the existing metadata and the new metadata.
 * @author yingjianw
 * @since 1.2.0
 */
public interface MetadataPreMergeInterceptor {
    /**
     * Validate ObjectNode before storing it.
     * @param userMetadataService user metadata service
     * @param name                qualified name
     * @param existingMetadata    existing metadata
     * @param newMetadata         newMetadata
     * @param throwException  whether to throw exception from the interceptor
     * @throws InvalidMetadataException business validation exception
     */
    default void onWrite(final UserMetadataService userMetadataService,
                         final QualifiedName name,
                         final Optional<ObjectNode> existingMetadata,
                         final Optional<ObjectNode> newMetadata,
                         final boolean throwException) throws InvalidMetadataException {

    }
}
