package com.netflix.metacat.connector.polaris.mappers;

import com.google.common.collect.ImmutableSet;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.connector.sql.DirectSqlTable;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Table object mapper implementations.
 */
public class PolarisTableMapper implements
    EntityToInfoMapper<PolarisTableEntity, TableInfo>,
    InfoToEntityMapper<TableInfo, PolarisTableEntity> {

    private static final String PARAMETER_SPARK_SQL_PROVIDER = "spark.sql.sources.provider";
    private static final String PARAMETER_EXTERNAL = "EXTERNAL";
    private static final String PARAMETER_METADATA_PREFIX = "/metadata/";
    private static final Set<String> EXCLUDED_PARAM_KEYS = ImmutableSet.of(
        DirectSqlTable.PARAM_METADATA_LOCATION,
        DirectSqlTable.PARAM_PREVIOUS_METADATA_LOCATION,
        DirectSqlTable.PARAM_TABLE_TYPE,
        DirectSqlTable.PARAM_PARTITION_SPEC,
        DirectSqlTable.PARAM_METADATA_CONTENT
    );

    /**
     * {@inheritDoc}.
     */
    @Override
    public TableInfo toInfo(final PolarisTableEntity entity) {
        return toInfo(entity, false);
    }

    /**
     * Maps an Entity to the Info object.
     *
     * @param entity The entity to map from.
     * @param isView Whether the given entity represents a common view
     * @return The result info object.
     */
    public TableInfo toInfo(final PolarisTableEntity entity, final boolean isView) {
        final int uriIndex = entity.getMetadataLocation().indexOf(PARAMETER_METADATA_PREFIX);

        final HashMap<String, String> metadata = new HashMap<>();
        metadata.put(DirectSqlTable.PARAM_METADATA_LOCATION, entity.getMetadataLocation());

        if (isView) {
            metadata.putAll(entity.getParams());
        } else {
            metadata.put(PARAMETER_EXTERNAL, "TRUE");
            metadata.put(PARAMETER_SPARK_SQL_PROVIDER, "iceberg");
            metadata.put(DirectSqlTable.PARAM_TABLE_TYPE, DirectSqlTable.ICEBERG_TABLE_TYPE);
        }

        final TableInfo tableInfo = TableInfo.builder()
            .name(QualifiedName.ofTable(entity.getCatalogName(), entity.getDbName(), entity.getTblName()))
            .metadata(metadata)
            .serde(StorageInfo.builder().inputFormat("org.apache.hadoop.mapred.FileInputFormat")
                .outputFormat("org.apache.hadoop.mapred.FileOutputFormat")
                .serializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
                .uri(uriIndex > 0 ? entity.getMetadataLocation().substring(0, uriIndex) : "")
                .build())
            .auditInfo(AuditInfo.builder()
                .createdBy(entity.getAudit().getCreatedBy())
                .createdDate(Date.from(entity.getAudit().getCreatedDate()))
                .lastModifiedBy(entity.getAudit().getLastModifiedBy())
                .lastModifiedDate(Date.from(entity.getAudit().getLastModifiedDate()))
                .build())
            .build();
        return tableInfo;
    }

    /**
     * Given TableInfo.metadata, filter out reserved metadata keys which should not be stored as table params.
     * @param metadata TableInfo.metadata
     * @return Map of table params which can be stored in PolarisTableEntity.params
     */
    public Map<String, String> filterMetadata(final Map<String, String> metadata) {
        return metadata.entrySet()
            .stream().filter(entry -> !EXCLUDED_PARAM_KEYS.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
            .catalogName(info.getName().getCatalogName())
            .dbName(info.getName().getDatabaseName())
            .tblName(info.getName().getTableName())
            .metadataLocation(location)
            .params(filterMetadata(metadata))
            .build();
        return tableEntity;
    }
}
