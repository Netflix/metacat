package com.netflix.metacat.common.server.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;

/**
 * MetadataSQLInterceptorImpl.
 *
 * @author yingjianw
 * @since 1.2.0
 */
public class MetadataSqlInterceptorImpl implements MetadataSqlInterceptor {
    @Override
    public String interceptSQL(
        final String sql,
        final QualifiedName name,
        final ObjectNode existingMetadata,
        final ObjectNode newMetadata) {
        return sql;
    }

    @Override
    public String failureMessage(final QualifiedName name,
                                 final UserMetadataService metadataService,
                                 final ObjectNode existing,
                                 final ObjectNode newMetadata) {
        return "";
    }
}
