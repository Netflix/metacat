package com.netflix.metacat.common.server.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;

/**
 * MetadataSQLInterceptor.
 *
 * @author yingjianw
 * @since 1.2.0
 */

public interface MetadataSqlInterceptor {
    /**
     * Intercept the definition metadata sql update statement into the db.
     * @param sql base sql statement
     * @param name qualifiedName
     * @param existingMetadata existingMetadata
     * @param newMetadata newMetadata
     * @return the modified sql string
     */
    String interceptSQL(String sql, QualifiedName name, ObjectNode existingMetadata, ObjectNode newMetadata);

    /**
     * Intercept the newMetadata during write.
     * @param name qualifiedName
     * @param metadataService metadataService
     * @param existing existing definition metadata in the db
     * @param newMetadata new definition metadata
     * @return Failure Message String
     *
     */
    String failureMessage(
        QualifiedName name,
        UserMetadataService metadataService,
        ObjectNode existing,
        ObjectNode newMetadata);
}
