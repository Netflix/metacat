package com.netflix.metacat.connector.polaris.mappers;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;

/**
 * Table object mapper implementations.
 */
public class PolarisTableMapper implements
    EntityToInfoMapper<PolarisTableEntity, TableInfo>,
    InfoToEntityMapper<TableInfo, PolarisTableEntity> {

    // TODO: this can be reworked if PolarisTableEntity starts tracking catalog name
    private final String catalogName;

    /**
     * Constructor.
     * @param catalogName the catalog name
     */
    public PolarisTableMapper(final String catalogName) {
        this.catalogName = catalogName;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public TableInfo toInfo(final PolarisTableEntity entity) {
        final TableInfo tableInfo = TableInfo.builder()
            .name(QualifiedName.ofTable(catalogName, entity.getDbName(), entity.getTblName()))
            .build();
        return tableInfo;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public PolarisTableEntity toEntity(final TableInfo info) {
        final PolarisTableEntity tableEntity = PolarisTableEntity.builder()
            .dbName(info.getName().getDatabaseName())
            .tblName(info.getName().getTableName())
            .build();
        return tableEntity;
    }
}
