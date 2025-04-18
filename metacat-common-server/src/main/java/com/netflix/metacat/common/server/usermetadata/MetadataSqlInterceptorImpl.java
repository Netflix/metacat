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
    public String interceptSQL(final String sql) {
        return sql;
    }

    @Override
    public void onWrite(final QualifiedName name,
                        final ObjectNode existing,
                        final ObjectNode newMetadata,
                        final UserMetadataService userMetadataService
    ) {

    }

    @Override
    public String failureMessage(final QualifiedName name, final ObjectNode existing, final ObjectNode newMetadata) {
        return "";
    }
}
