package com.netflix.metacat.connector.polaris.mappers;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;

/**
 * Database object mapper implementations.
 */
public class PolarisDatabaseMapper implements
    EntityToInfoMapper<PolarisDatabaseEntity, DatabaseInfo>,
    InfoToEntityMapper<DatabaseInfo, PolarisDatabaseEntity> {

    /**
     * {@inheritDoc}.
     */
    @Override
    public DatabaseInfo toInfo(final PolarisDatabaseEntity entity) {
        final AuditMapper mapper = new AuditMapper();
        final DatabaseInfo databaseInfo = DatabaseInfo.builder()
            .name(QualifiedName.ofDatabase(entity.getCatalogName(), entity.getDbName()))
            .auditInfo(mapper.toInfo(entity.getAudit()))
            .metadata(entity.getParams())
            .uri(entity.getLocation())
            .build();
        return databaseInfo;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public PolarisDatabaseEntity toEntity(final DatabaseInfo info) {
        final PolarisDatabaseEntity databaseEntity = PolarisDatabaseEntity.builder()
            .catalogName(info.getName().getCatalogName())
            .dbName(info.getName().getDatabaseName())
            .location(info.getUri())
            .params(info.getMetadata())
            .build();
        return databaseEntity;
    }
}
