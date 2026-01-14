package com.netflix.metacat.connector.polaris;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.PartitionNotFoundException;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.connector.iceberg.IcebergTableHandler;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Partition service for Iceberg tables in Polaris.
 *
 * Currently, supports read-only methods with the exception of getPartitionNames.
 */
@RequiredArgsConstructor
public class PolarisConnectorPartitionService implements ConnectorPartitionService {
    private final ConnectorContext context;
    private final IcebergTableHandler icebergTableHandler;
    private final PolarisConnectorTableService tableService;

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<PartitionInfo> getPartitions(@NonNull final ConnectorRequestContext requestContext,
                                             @NonNull final QualifiedName tableName,
                                             @NonNull final PartitionListRequest partitionsRequest,
                                             @NonNull final TableInfo tableInfo) {
        return ConnectorUtils.paginate(
            icebergTableHandler.getPartitions(
                tableInfo,
                context,
                partitionsRequest.getFilter(),
                partitionsRequest.getPartitionNames(),
                partitionsRequest.getSort()
            ),
            partitionsRequest.getPageable()
        );
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionKeys(@NonNull final ConnectorRequestContext requestContext,
                                         @NonNull final QualifiedName tableName,
                                         @NonNull final PartitionListRequest partitionsRequest,
                                         @NonNull final TableInfo tableInfo) {
        return getPartitions(requestContext, tableName, partitionsRequest, tableInfo).stream()
                   .map(info -> info.getName().getPartitionName())
                   .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int getPartitionCount(@NonNull final ConnectorRequestContext requestContext,
                                 @NonNull final QualifiedName table,
                                 @NonNull final TableInfo tableInfo) {
        return icebergTableHandler.getPartitions(
            tableInfo,
            context,
            null, // filer expression
            null, // partition ids
            null // sort
        ).size();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionUris(@NonNull final ConnectorRequestContext requestContext,
                                         @NonNull final QualifiedName table,
                                         @NonNull final PartitionListRequest partitionsRequest,
                                         @NonNull final TableInfo tableInfo) {
        return getPartitions(requestContext, table, partitionsRequest, tableInfo).stream()
                   .map(partitionInfo -> partitionInfo.getSerde().getUri())
                   .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public PartitionInfo get(@NonNull final ConnectorRequestContext requestContext,
                             @NonNull final QualifiedName partitionName) {
        final QualifiedName tableName = QualifiedName.ofTable(
            partitionName.getCatalogName(),
            partitionName.getDatabaseName(),
            partitionName.getTableName()
        );

        final TableInfo tableInfo = tableService.get(requestContext, tableName);

        final List<PartitionInfo> partitions = icebergTableHandler.getPartitions(
            tableInfo,
            context,
            null,
            Collections.singletonList(partitionName.getPartitionName()),
            null
        );

        return partitions.stream()
                   .filter(partitionInfo -> partitionInfo.getName().equals(partitionName))
                   .findFirst()
                   .orElseThrow(() -> new PartitionNotFoundException(tableName, partitionName.getPartitionName()));
    }
}
