package com.netflix.metacat.connector.polaris.mappers;

import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.sql.DirectSqlTable;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * Table object mapper implementations.
 */
public class PolarisTableMapper implements
    EntityToInfoMapper<PolarisTableEntity, TableInfo>,
    InfoToEntityMapper<TableInfo, PolarisTableEntity> {

    private static final String PARAMETER_SPARK_SQL_PROVIDER = "spark.sql.sources.provider";
    private static final String PARAMETER_EXTERNAL = "EXTERNAL";
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
            .metadata(ImmutableMap.of(
                DirectSqlTable.PARAM_METADATA_LOCATION, entity.getMetadataLocation(),
                PARAMETER_EXTERNAL, "TRUE", PARAMETER_SPARK_SQL_PROVIDER, "iceberg",
                DirectSqlTable.PARAM_TABLE_TYPE, DirectSqlTable.ICEBERG_TABLE_TYPE))
            .serde(StorageInfo.builder().inputFormat("org.apache.hadoop.mapred.FileInputFormat")
                .outputFormat("org.apache.hadoop.mapred.FileOutputFormat")
                .serializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
                .build())
            .build();
        return tableInfo;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public PolarisTableEntity toEntity(final TableInfo info) {
        final Map<String, String> metadata = info.getMetadata();
        if (MapUtils.isEmpty(metadata)) {
            final String message = String.format("No parameters defined for iceberg table %s", info.getName());
            throw new InvalidMetaException(info.getName(), message, null);
        }
        final String location = metadata.get(DirectSqlTable.PARAM_METADATA_LOCATION);
        if (StringUtils.isEmpty(location)) {
            final String message = String.format("No metadata location defined for iceberg table %s", info.getName());
            throw new InvalidMetaException(info.getName(), message, null);
        }
        final PolarisTableEntity tableEntity = PolarisTableEntity.builder()
            .dbName(info.getName().getDatabaseName())
            .tblName(info.getName().getTableName())
            .metadataLocation(location)
            .build();
        return tableEntity;
    }
}
