//CHECKSTYLE:OFF
package com.netflix.metacat.metadata.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.metadata.store.data.entities.AuditEntity;
import com.netflix.metacat.metadata.store.data.entities.DataMetadataEntity;
import com.netflix.metacat.metadata.store.data.entities.DefinitionMetadataEntity;

import java.time.Instant;

public class EntityTestUtil {
    public static ObjectMapper objectMapper = new ObjectMapper()
            .findAndRegisterModules()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setSerializationInclusion(JsonInclude.Include.ALWAYS);

    public static DataMetadataEntity createDataMetadataEntity() {
        return createDataMetadataEntity("s3://iceberg/bucket");
    }

    public static DataMetadataEntity createDataMetadataEntity(String uri) {
        return DataMetadataEntity.builder()
                .uri(uri)
                .data(createTestObjectNode())
                .audit(createAuditEntity())
                .build();
    }

    public static DefinitionMetadataEntity createDefinitionMetadataEntity() {
        return createDefinitionMetadataEntity(QualifiedName.fromString("prodhive/foo/bar"));
    }

    public static DefinitionMetadataEntity createDefinitionMetadataEntity(QualifiedName name) {
        return DefinitionMetadataEntity.builder()
                .name(name)
                .data(createTestObjectNode())
                .audit(createAuditEntity())
                .build();
    }

    public static AuditEntity createAuditEntity() {
        return AuditEntity.builder()
                .createdBy("metacat_user")
                .lastModifiedBy("metacat_user")
                .createdDate(Instant.now())
                .lastModifiedDate(Instant.now())
                .build();
    }

    public static ObjectNode createTestObjectNode() {
        return objectMapper.createObjectNode().put("size", "50");
    }
}
