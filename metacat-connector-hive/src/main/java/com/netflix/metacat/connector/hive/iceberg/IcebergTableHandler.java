/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.connector.hive.iceberg;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.exception.TablePreconditionFailedException;
import com.netflix.metacat.common.server.connectors.model.AuditInfo;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionInfo;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.partition.parser.ParseException;
import com.netflix.metacat.common.server.partition.parser.PartitionParser;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.metacat.connector.hive.sql.DirectSqlGetPartition;
import com.netflix.metacat.connector.hive.sql.DirectSqlTable;
import com.netflix.metacat.connector.hive.util.HiveTableUtil;
import com.netflix.metacat.connector.hive.util.IcebergFilterGenerator;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ScanSummary;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Iceberg table handler which interacts with iceberg library
 * to perform iceberg table loading, querying, etc. The operations limit to
 * read-only for now.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Slf4j
public class IcebergTableHandler {
    private static final Retryer<Void> RETRY_ICEBERG_TABLE_UPDATE = RetryerBuilder.<Void>newBuilder()
        .retryIfExceptionOfType(TablePreconditionFailedException.class)
        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
        .build();
    private final Configuration conf;
    private final ConnectorContext connectorContext;
    private final Registry registry;
    @VisibleForTesting
    private IcebergTableCriteria icebergTableCriteria;
    @VisibleForTesting
    private IcebergTableOpWrapper icebergTableOpWrapper;
    private IcebergTableOpsProxy icebergTableOpsProxy;

    /**
     * Constructor.
     *
     * @param connectorContext      connector context
     * @param icebergTableCriteria  iceberg table criteria
     * @param icebergTableOpWrapper iceberg table operation
     * @param icebergTableOpsProxy  IcebergTableOps proxy
     */
    public IcebergTableHandler(final ConnectorContext connectorContext,
                               final IcebergTableCriteria icebergTableCriteria,
                               final IcebergTableOpWrapper icebergTableOpWrapper,
                               final IcebergTableOpsProxy icebergTableOpsProxy) {
        this.conf = new Configuration();
        this.connectorContext = connectorContext;
        this.registry = connectorContext.getRegistry();
        connectorContext.getConfiguration().keySet()
            .forEach(key -> conf.set(key, connectorContext.getConfiguration().get(key)));
        this.icebergTableCriteria = icebergTableCriteria;
        this.icebergTableOpWrapper = icebergTableOpWrapper;
        this.icebergTableOpsProxy = icebergTableOpsProxy;
    }

    /**
     * Returns the partitions for the given table and filter.
     *
     * @param tableInfo the table info
     * @param context the request context
     * @param filterExpression the filter expression
     * @param partitionIds the partition ids to match
     * @param sort the sort order
     * @return the list of partitions
     */
    public List<PartitionInfo> getPartitions(final TableInfo tableInfo,
                                             final ConnectorContext context,
                                             @Nullable final String filterExpression,
                                             @Nullable final List<String> partitionIds,
                                             @Nullable final Sort sort) {
        final QualifiedName tableName = tableInfo.getName();
        final org.apache.iceberg.Table icebergTable = getIcebergTable(tableName,
            HiveTableUtil.getIcebergTableMetadataLocation(tableInfo), false).getTable();

        final Map<String, ScanSummary.PartitionMetrics> partitionMap
            = getIcebergTablePartitionMap(tableName, filterExpression, icebergTable);

        final AuditInfo tableAuditInfo = tableInfo.getAudit();

        final List<PartitionInfo> filteredPartitionList = partitionMap.keySet().stream()
              .filter(partitionName -> partitionIds == null || partitionIds.contains(partitionName))
              .map(partitionName ->
                       PartitionInfo.builder().name(
                          QualifiedName.ofPartition(tableName.getCatalogName(),
                              tableName.getDatabaseName(),
                              tableName.getTableName(),
                              partitionName)
                      ).serde(StorageInfo.builder().uri(
                          getIcebergPartitionURI(
                              tableName.getDatabaseName(),
                              tableName.getTableName(),
                              partitionName,
                              partitionMap.get(partitionName).dataTimestampMillis(),
                              context
                          )).build()
                      )
                      .dataMetrics(getDataMetadataFromIcebergMetrics(partitionMap.get(partitionName)))
                      .auditInfo(
                          AuditInfo.builder()
                             .createdBy(tableAuditInfo.getCreatedBy())
                             .createdDate(fromEpochMilliToDate(partitionMap.get(partitionName).dataTimestampMillis()))
                             .lastModifiedDate(
                                 fromEpochMilliToDate(partitionMap.get(partitionName).dataTimestampMillis()))
                             .build()
                      ).build()
              )
              .collect(Collectors.toList());

        if (sort != null) {
            if (sort.hasSort() && sort.getSortBy().equalsIgnoreCase(DirectSqlGetPartition.FIELD_DATE_CREATED)) {
                final Comparator<PartitionInfo> dateCreatedComparator = Comparator.comparing(
                    p -> p.getAudit() != null ? p.getAudit().getCreatedDate() : null,
                    Comparator.nullsLast(Date::compareTo));

                ConnectorUtils.sort(filteredPartitionList, sort, dateCreatedComparator);
            } else {
                // Sort using the partition name by default
                final Comparator<PartitionInfo> nameComparator = Comparator.comparing(p -> p.getName().toString());
                ConnectorUtils.sort(filteredPartitionList, sort, nameComparator);
            }
        }

        return filteredPartitionList;
    }

    /**
     * get Partition Map.
     *
     * @param tableName         Qualified table name
     * @param filterExpression  the filter
     * @param icebergTable      iceberg Table
     * @return partition map
     */
    public Map<String, ScanSummary.PartitionMetrics> getIcebergTablePartitionMap(
        final QualifiedName tableName,
        @Nullable final String filterExpression,
        final Table icebergTable) {
        final long start = this.registry.clock().wallTime();
        final Map<String, ScanSummary.PartitionMetrics> result;
        try {
            if (!Strings.isNullOrEmpty(filterExpression)) {
                final IcebergFilterGenerator icebergFilterGenerator
                    = new IcebergFilterGenerator(icebergTable.schema().columns());
                final Expression filter = (Expression) new PartitionParser(
                    new StringReader(filterExpression)).filter()
                    .jjtAccept(icebergFilterGenerator, null);
                result = this.icebergTableOpWrapper.getPartitionMetricsMap(icebergTable, filter);
            } else {
                result = this.icebergTableOpWrapper.getPartitionMetricsMap(icebergTable, null);
            }
        } catch (ParseException ex) {
            log.error("Iceberg filter parse error: ", ex);
            throw new IllegalArgumentException(String.format("Iceberg filter parse error. Ex: %s", ex.getMessage()));
        } catch (IllegalStateException e) {
            registry.counter(registry.createId(IcebergRequestMetrics.CounterGetPartitionsExceedThresholdFailure
                .getMetricName()).withTags(tableName.parts())).increment();
            final String message =
                String.format("Number of partitions queried for table %s exceeded the threshold %d",
                    tableName, connectorContext.getConfig().getMaxPartitionsThreshold());
            log.warn(message);
            throw new IllegalArgumentException(message);
        } finally {
            final long duration = registry.clock().wallTime() - start;
            log.info("Time taken to getIcebergTablePartitionMap {} is {} ms", tableName, duration);
            this.recordTimer(
                IcebergRequestMetrics.TagGetPartitionMap.getMetricName(), duration);
            this.increaseCounter(
                IcebergRequestMetrics.TagGetPartitionMap.getMetricName(), tableName);
        }

        return result;
    }


    /**
     * get iceberg table.
     *
     * @param tableName             table name
     * @param tableMetadataLocation table metadata location
     * @param includeInfoDetails    if true, will include more details like the manifest file content
     * @return iceberg table
     */
    public IcebergTableWrapper getIcebergTable(final QualifiedName tableName, final String tableMetadataLocation,
                                               final boolean includeInfoDetails) {
        final long start = this.registry.clock().wallTime();
        try {
            this.icebergTableCriteria.checkCriteria(tableName, tableMetadataLocation);
            log.debug("Loading icebergTable {} from {}", tableName, tableMetadataLocation);
            final IcebergMetastoreTables icebergMetastoreTables = new IcebergMetastoreTables(
                new IcebergTableOps(conf, tableMetadataLocation, tableName.getTableName(),
                    connectorContext.getConfig(),
                    icebergTableOpsProxy));
            final Table table = icebergMetastoreTables.loadTable(
                HiveTableUtil.qualifiedNameToTableIdentifier(tableName));
            final Map<String, String> extraProperties = Maps.newHashMap();
            if (includeInfoDetails) {
                extraProperties.put(DirectSqlTable.PARAM_METADATA_CONTENT,
                    TableMetadataParser.toJson(icebergMetastoreTables.getTableOps().current()));
            }
            return new IcebergTableWrapper(table, extraProperties);
        } catch (NotFoundException | NoSuchTableException e) {
            throw new InvalidMetaException(tableName, e);
        } finally {
            final long duration = registry.clock().wallTime() - start;
            log.info("Time taken to getIcebergTable {} is {} ms", tableName, duration);
            this.recordTimer(IcebergRequestMetrics.TagLoadTable.getMetricName(), duration);
            this.increaseCounter(IcebergRequestMetrics.TagLoadTable.getMetricName(), tableName);
        }
    }

    /**
     * Updates the iceberg schema if the provided tableInfo has updated field comments.
     *
     * @param tableInfo table information
     * @return true if an update is done
     */
    public boolean update(final TableInfo tableInfo) {
        boolean result = false;
        final List<FieldInfo> fields = tableInfo.getFields();
        if (fields != null && !fields.isEmpty()
            // This parameter is only sent during data change and not during schema change.
            && Strings.isNullOrEmpty(tableInfo.getMetadata().get(DirectSqlTable.PARAM_PREVIOUS_METADATA_LOCATION))) {
            final QualifiedName tableName = tableInfo.getName();
            final String tableMetadataLocation = HiveTableUtil.getIcebergTableMetadataLocation(tableInfo);
            if (Strings.isNullOrEmpty(tableMetadataLocation)) {
                final String message = String.format("No metadata location specified for table %s", tableName);
                log.error(message);
                throw new MetacatBadRequestException(message);
            }
            final IcebergMetastoreTables icebergMetastoreTables = new IcebergMetastoreTables(
                new IcebergTableOps(conf, tableMetadataLocation, tableName.getTableName(),
                    connectorContext.getConfig(),
                    icebergTableOpsProxy));
            final Table table = icebergMetastoreTables.loadTable(
                HiveTableUtil.qualifiedNameToTableIdentifier(tableName));
            final UpdateSchema updateSchema = table.updateSchema();
            final Schema schema = table.schema();
            for (FieldInfo field : fields) {
                final Types.NestedField iField = schema.findField(field.getName());
                if (iField != null && !Objects.equals(field.getComment(), iField.doc())) {
                    updateSchema.updateColumnDoc(field.getName(), field.getComment());
                    result = true;
                }
            }
            if (result) {
                updateSchema.commit();
                final String newTableMetadataLocation = icebergMetastoreTables.getTableOps().currentMetadataLocation();
                if (!tableMetadataLocation.equalsIgnoreCase(newTableMetadataLocation)) {
                    tableInfo.getMetadata().put(DirectSqlTable.PARAM_PREVIOUS_METADATA_LOCATION, tableMetadataLocation);
                    tableInfo.getMetadata().put(DirectSqlTable.PARAM_METADATA_LOCATION,
                        newTableMetadataLocation);
                }
            }
        }
        return result;
    }

    /**
     * Handle iceberg table update operation.
     *
     * @param requestContext request context
     * @param directSqlTable direct sql table object
     * @param tableInfo      table info
     */
    public void handleUpdate(final ConnectorRequestContext requestContext,
                             final DirectSqlTable directSqlTable,
                             final TableInfo tableInfo) {
        requestContext.setIgnoreErrorsAfterUpdate(true);
        this.update(tableInfo);
        // TODO: only trying once for correctness for now to fix a race condition that could lead to data loss
        // but this needs more retries in case of schema updates for better user experience
        directSqlTable.updateIcebergTable(tableInfo);
    }

    /**
     * get data metadata from partition metrics.
     *
     * @param metrics metrics.
     * @return object node of the metrics
     */
    public ObjectNode getDataMetadataFromIcebergMetrics(
        final ScanSummary.PartitionMetrics metrics) {
        final ObjectNode root = JsonNodeFactory.instance.objectNode();
        root.set(DataMetadataMetricConstants.DATA_METADATA_METRIC_NAME, getMetricValueNode(metrics));
        return root;
    }

    /**
     * Checks if the given iceberg table metadata location exists.
     *
     * @param tableName The table name.
     * @param metadataLocation The metadata location.
     * @return True if the location exists.
     */
    public boolean doesMetadataLocationExist(final QualifiedName tableName,
                                             final String metadataLocation) {
        boolean result = false;
        if (!StringUtils.isBlank(metadataLocation)) {
            try {
                final Path metadataPath = new Path(metadataLocation);
                result = getFs(metadataPath, conf).exists(metadataPath);
            } catch (Exception ignored) {
                log.warn(String.format("Failed getting the filesystem for metadata location: %s tableName: %s",
                        metadataLocation, tableName));
                registry.counter(HiveMetrics.CounterFileSystemReadFailure.name()).increment();
            }
        }
        return result;
    }

    private static FileSystem getFs(final Path path,
                                    final Configuration conf) {
        try {
            return path.getFileSystem(conf);
        } catch (IOException ex) {
            throw new RuntimeException(String.format("Failed to get file system for path: %s", path));
        }
    }

    private ObjectNode getMetricValueNode(final ScanSummary.PartitionMetrics metrics) {
        final ObjectNode node = JsonNodeFactory.instance.objectNode();

        ObjectNode valueNode = JsonNodeFactory.instance.objectNode();
        valueNode.put(DataMetadataMetricConstants.DATA_METADATA_VALUE, metrics.recordCount());
        node.set(DataMetadataMetrics.rowCount.getMetricName(), valueNode);

        valueNode = JsonNodeFactory.instance.objectNode();
        valueNode.put(DataMetadataMetricConstants.DATA_METADATA_VALUE, metrics.fileCount());
        node.set(DataMetadataMetrics.fileCount.getMetricName(), valueNode);
        return node;
    }

    /**
     * record the duration to timer.
     *
     * @param requestTag tag name.
     * @param duration   duration of the operation.
     */
    private void recordTimer(final String requestTag, final long duration) {
        final HashMap<String, String> tags = new HashMap<>();
        tags.put("request", requestTag);
        this.registry.timer(registry.createId(IcebergRequestMetrics.TimerIcebergRequest.getMetricName())
            .withTags(tags))
            .record(duration, TimeUnit.MILLISECONDS);
        log.debug("## Time taken to complete {} is {} ms", requestTag, duration);
    }

    /**
     * increase the counter of operation.
     *
     * @param metricName metric name
     * @param tableName  table name of the operation
     */
    private void increaseCounter(final String metricName, final QualifiedName tableName) {
        this.registry.counter(registry.createId(metricName).withTags(tableName.parts())).increment();
    }

    private Date fromEpochMilliToDate(@Nullable final Long l) {
        return (l == null) ? null : Date.from(Instant.ofEpochMilli(l));
    }

    //iceberg://<db-name.table-name>/<partition>/snapshot_time=<dateCreated>
    private String getIcebergPartitionURI(final String databaseName,
                                          final String tableName,
                                          final String partitionName,
                                          @Nullable final Long dataTimestampMillis,
                                          final ConnectorContext context) {
        return String.format("%s://%s.%s/%s/snapshot_time=%s",
            context.getConfig().getIcebergPartitionUriScheme(),
            databaseName,
            tableName,
            partitionName,
            (dataTimestampMillis == null) ? partitionName.hashCode()
                : Instant.ofEpochMilli(dataTimestampMillis).getEpochSecond());
    }
}
