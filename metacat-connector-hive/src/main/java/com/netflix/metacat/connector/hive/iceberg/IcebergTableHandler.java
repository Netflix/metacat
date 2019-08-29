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
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.netflix.iceberg.BaseMetastoreTableOperations;
import com.netflix.iceberg.BaseMetastoreTables;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.ScanSummary;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableMetadata;
import com.netflix.iceberg.TableMetadataParser;
import com.netflix.iceberg.exceptions.NoSuchTableException;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.partition.parser.ParseException;
import com.netflix.metacat.common.server.partition.parser.PartitionParser;
import com.netflix.metacat.connector.hive.sql.DirectSqlTable;
import com.netflix.metacat.connector.hive.util.IcebergFilterGenerator;
import com.netflix.servo.util.VisibleForTesting;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
    private final Configuration conf;
    private final ConnectorContext connectorContext;
    private final Registry registry;
    @VisibleForTesting
    private IcebergTableCriteria icebergTableCriteria;
    @VisibleForTesting
    private IcebergTableOpWrapper icebergTableOpWrapper;

    /**
     * Constructor.
     *
     * @param connectorContext     connector context
     * @param icebergTableCriteria iceberg table criteria
     * @param icebergTableOpWrapper iceberg table operation
     */
    public IcebergTableHandler(final ConnectorContext connectorContext,
                               final IcebergTableCriteria icebergTableCriteria,
                               final IcebergTableOpWrapper icebergTableOpWrapper) {
        this.conf = new Configuration();
        this.connectorContext = connectorContext;
        this.registry = connectorContext.getRegistry();
        connectorContext.getConfiguration().keySet()
            .forEach(key -> conf.set(key, connectorContext.getConfiguration().get(key)));
        this.icebergTableCriteria = icebergTableCriteria;
        this.icebergTableOpWrapper = icebergTableOpWrapper;
    }

    /**
     * get Partition Map.
     *
     * @param tableName         Qualified table name
     * @param partitionsRequest partitionsRequest
     * @param icebergTable      iceberg Table
     * @return partition map
     */
    public Map<String, ScanSummary.PartitionMetrics> getIcebergTablePartitionMap(
        final QualifiedName tableName,
        final PartitionListRequest partitionsRequest,
        final Table icebergTable) {
        final long start = this.registry.clock().wallTime();
        final Map<String, ScanSummary.PartitionMetrics> result;
        try {
            if (!Strings.isNullOrEmpty(partitionsRequest.getFilter())) {
                final IcebergFilterGenerator icebergFilterGenerator
                    = new IcebergFilterGenerator(icebergTable.schema().columns());
                final Expression filter = (Expression) new PartitionParser(
                    new StringReader(partitionsRequest.getFilter())).filter()
                    .jjtAccept(icebergFilterGenerator, null);
                result = this.icebergTableOpWrapper.getPartitionMetricsMap(icebergTable, filter);
            } else {
                result = this.icebergTableOpWrapper.getPartitionMetricsMap(icebergTable, null);
            }
        } catch (ParseException ex) {
            log.error("Iceberg filter parse error", ex);
            throw new MetacatBadRequestException("Iceberg filter parse error");
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
            final IcebergMetastoreTables icebergMetastoreTables = new IcebergMetastoreTables(tableMetadataLocation);
            final Table table = icebergMetastoreTables.load(tableName.toString());
            final Map<String, String> extraProperties = Maps.newHashMap();
            if (includeInfoDetails) {
                extraProperties.put(DirectSqlTable.PARAM_METADATA_CONTENT,
                    TableMetadataParser.toJson(icebergMetastoreTables.getTableOps().current()));
            }
            return new IcebergTableWrapper(table, extraProperties);
        } catch (NoSuchTableException e) {
            throw new InvalidMetaException(tableName, e);
        } finally {
            final long duration = registry.clock().wallTime() - start;
            log.info("Time taken to getIcebergTable {} is {} ms", tableName, duration);
            this.recordTimer(IcebergRequestMetrics.TagLoadTable.getMetricName(), duration);
            this.increaseCounter(IcebergRequestMetrics.TagLoadTable.getMetricName(), tableName);
        }
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
     * Implemented BaseMetastoreTables to interact with iceberg library.
     * Load an iceberg table from a location.
     */
    private final class IcebergMetastoreTables extends BaseMetastoreTables {
        private final String tableLocation;
        private BaseMetastoreTableOperations tableOperations;

        IcebergMetastoreTables(final String tableMetadataLocation) {
            super(conf);
            this.tableLocation = tableMetadataLocation;
        }

        @Override
        public Table create(final Schema schema, final PartitionSpec spec, final String database, final String table) {
            throw new MetacatNotSupportedException("not supported");
        }

        @Override
        public Table create(final Schema schema,
                            final PartitionSpec spec,
                            final Map<String, String> properties,
                            final String table) {
            throw new MetacatNotSupportedException("Not supported");
        }

        @Override
        public Table create(final Schema schema, final PartitionSpec spec, final String tables) {
            throw new MetacatNotSupportedException("Not supported");
        }

        @Override
        public Table load(final String tableName) {
            final QualifiedName table = QualifiedName.fromString(tableName);
            return super.load(table.getDatabaseName(), table.getTableName());
        }

        @Override
        public BaseMetastoreTableOperations newTableOps(final Configuration config,
                                                        final String database,
                                                        final String table) {
            return getTableOps();
        }

        /**
         * Creates a new instance of IcebergTableOps for the given table location, if not exists.
         *
         * @return a MetacatServerOps for the table
         */
        public BaseMetastoreTableOperations getTableOps() {
            if (tableOperations == null) {
                tableOperations = new IcebergTableOps(tableLocation);
            }
            return tableOperations;
        }
    }

    /**
     * Implemented the BaseMetastoreTableOperations to interact with iceberg library.
     * Read only operations.
     */
    private final class IcebergTableOps extends BaseMetastoreTableOperations {
        private final String location;

        IcebergTableOps(final String location) {
            super(conf);
            this.location = location;
            refresh();
        }

        @Override
        public TableMetadata refresh() {
            refreshFromMetadataLocation(this.location,
                connectorContext.getConfig().getIcebergRefreshFromMetadataLocationRetryNumber());
            return current();
        }

        /**
         * No-op operation.
         */
        @Override
        public void commit(final TableMetadata base, final TableMetadata metadata) {
        }
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
     * @param metricName metric name
     * @param tableName table name of the operation
     */
    private void increaseCounter(final String metricName, final QualifiedName tableName) {
        this.registry.counter(registry.createId(metricName).withTags(tableName.parts())).increment();
    }
}
