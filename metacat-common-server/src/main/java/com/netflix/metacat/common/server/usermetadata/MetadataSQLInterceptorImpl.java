package com.netflix.metacat.common.server.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;

public class MetadataSQLInterceptorImpl implements MetadataSQLInterceptor {
    @Override
    public String interceptSQL(String sql) {
        return "";
    }

    @Override
    public void onWrite(QualifiedName name, ObjectNode existing, ObjectNode newMetadata, UserMetadataService userMetadataService) {

    }
}
