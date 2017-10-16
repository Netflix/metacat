package com.netflix.metacat.connector.druid.converter;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.connector.druid.DruidConfigConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Druid Info Converter.
 *
 * @author zhenl
 * @since 1.2.0
 */
public class DruidConnectorInfoConverter implements ConnectorInfoConverter<Database, Database, Segment> {
    private final String catalogName;

    /**
     * Constructor.
     *
     * @param catalogName catalog Name
     */
    public DruidConnectorInfoConverter(final String catalogName) {
        this.catalogName = catalogName;
    }

    /**
     * Convert from data source to partitionInfo.
     *
     * @param segment segment object
     * @return partition info object
     */
    public PartitionInfo getPartitionInfoFromSegment(final Segment segment) {
        final Map<String, String> metadata = new HashMap<>();
        metadata.put(DruidConfigConstants.LOADSPEC_KEY, segment.getLoadSpec().getKeys().toString());
        metadata.put(DruidConfigConstants.LOADSPEC_BUCKET, segment.getLoadSpec().getBucket());
        metadata.put(DruidConfigConstants.LOADSPEC_TYPE, segment.getLoadSpec().getType());
        metadata.put(DruidConfigConstants.DIMENSIONS, segment.getDimensions());
        metadata.put(DruidConfigConstants.METRICS, segment.getMetric());
        final StorageInfo storageInfo = StorageInfo.builder().uri(segment.getLoadSpec().getUri()).build();
        return PartitionInfo.builder().metadata(metadata).serde(storageInfo).build();
    }


    /**
     * Convert from data source to partitionInfo.
     *
     * @param dataSource dataSource object
     * @return table info object
     */
    public TableInfo getTableInfoFromDatasource(final DataSource dataSource) {
        final List<Segment> segmentList = dataSource.getSegmentList();
        final Segment latestSegment = segmentList.get(segmentList.size() - 1);
        final List<FieldInfo> fieldInfos = new ArrayList<>();

        for (String dim : latestSegment.getDimensions().split(",")) {
            fieldInfos.add(FieldInfo.builder()
                .comment(DruidConfigConstants.DIMENSIONS)
                .name(dim)
                .type(BaseType.STRING)
                .build());
        }
        for (String dim : latestSegment.getMetric().split(",")) {
            fieldInfos.add(FieldInfo.builder()
                .comment(DruidConfigConstants.METRICS)
                .name(dim)
                .type(BaseType.DOUBLE)
                .build());
        }

        return TableInfo.builder().fields(fieldInfos)
            .name(QualifiedName.ofTable(catalogName, DruidConfigConstants.DRUID_DB, dataSource.getName()))
            .build();
    }

}
