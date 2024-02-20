package com.netflix.metacat.connector.polaris.mappers;

import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.connector.polaris.store.entities.AuditEntity;
import org.apache.commons.lang3.ObjectUtils;

import java.util.Date;


/**
 * Audit object mapper implementations.
 */
public class AuditMapper implements
    EntityToInfoMapper<AuditEntity, AuditInfo>,
    InfoToEntityMapper<AuditInfo, AuditEntity> {

    /**
     * {@inheritDoc}.
     */
    @Override
    public AuditInfo toInfo(final AuditEntity entity) {
        final AuditInfo auditInfo = AuditInfo.builder()
            .createdBy(entity.getCreatedBy())
            .createdDate(Date.from(entity.getCreatedDate()))
            .lastModifiedBy(entity.getLastModifiedBy())
            .lastModifiedDate(Date.from(entity.getLastModifiedDate()))
            .build();
        return auditInfo;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public AuditEntity toEntity(final AuditInfo info) {
        final AuditEntity entity = AuditEntity.builder()
            .createdBy(info.getCreatedBy())
            .createdDate(ObjectUtils.defaultIfNull(info.getCreatedDate(), new Date()).toInstant())
            .lastModifiedBy(info.getLastModifiedBy())
            .lastModifiedDate(ObjectUtils.defaultIfNull(info.getLastModifiedDate(), new Date()).toInstant())
            .build();
        return entity;
    }
}
