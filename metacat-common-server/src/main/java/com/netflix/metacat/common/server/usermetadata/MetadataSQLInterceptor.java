package com.netflix.metacat.common.server.usermetadata;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;

public interface MetadataSQLInterceptor {
    public String interceptSQL(String sql);
    public void onWrite(QualifiedName name,
                        ObjectNode existing,
                        ObjectNode newMetadata,
                        final UserMetadataService userMetadataService
    );
}
