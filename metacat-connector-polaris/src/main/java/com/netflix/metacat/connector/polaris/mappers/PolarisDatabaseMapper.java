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

    // TODO: this can be reworked if PolarisDatabaseEntity starts tracking catalog name
    private final String catalogName;

    /**
     * Constructor.
     * @param catalogName the catalog name
     */
    public PolarisDatabaseMapper(final String catalogName) {
        this.catalogName = catalogName;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public DatabaseInfo toInfo(final PolarisDatabaseEntity entity) {
        final DatabaseInfo databaseInfo = DatabaseInfo.builder()
            .name(QualifiedName.ofDatabase(catalogName, entity.getDbName()))
            .build();
        return databaseInfo;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public PolarisDatabaseEntity toEntity(final DatabaseInfo info) {
        final PolarisDatabaseEntity databaseEntity = PolarisDatabaseEntity.builder()
            .dbName(info.getName().getDatabaseName())
            .build();
        return databaseEntity;
    }
}
