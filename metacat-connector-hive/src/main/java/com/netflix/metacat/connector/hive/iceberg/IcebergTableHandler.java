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
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatBadRequestException;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.partition.parser.ParseException;
import com.netflix.metacat.common.server.partition.parser.PartitionParser;
import com.netflix.metacat.connector.hive.sql.DirectSqlTable;
import com.netflix.metacat.connector.hive.util.HiveTableUtil;
import com.netflix.metacat.connector.hive.util.IcebergFilterGenerator;
import com.netflix.servo.util.VisibleForTesting;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanSummary;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
     * @param connectorContext      connector context
     * @param icebergTableCriteria  iceberg table criteria
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
            final IcebergMetastoreTables icebergMetastoreTables = new IcebergMetastoreTables(tableMetadataLocation);
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
    private final class IcebergMetastoreTables extends BaseMetastoreCatalog {
        private final String tableLocation;
        private IcebergTableOps tableOperations;

        IcebergMetastoreTables(final String tableMetadataLocation) {
            this.tableLocation = tableMetadataLocation;
        }

        @Override
        public Table createTable(final TableIdentifier identifier,
                                 final Schema schema,
                                 final PartitionSpec spec,
                                 final String location,
                                 final Map<String, String> properties) {
            throw new MetacatNotSupportedException("not supported");
        }

        @Override
        public Transaction newCreateTableTransaction(final TableIdentifier identifier,
                                                     final Schema schema,
                                                     final PartitionSpec spec,
                                                     final String location,
                                                     final Map<String, String> properties) {
            throw new MetacatNotSupportedException("not supported");
        }

        @Override
        public Transaction newReplaceTableTransaction(final TableIdentifier identifier,
                                                      final Schema schema,
                                                      final PartitionSpec spec,
                                                      final String location,
                                                      final Map<String, String> properties,
                                                      final boolean orCreate) {
            throw new MetacatNotSupportedException("not supported");
        }

        @Override
        public Table loadTable(final TableIdentifier identifier) {
            return super.loadTable(identifier);
        }

        @Override
        protected TableOperations newTableOps(final TableIdentifier tableIdentifier) {
            return getTableOps();
        }

        @Override
        protected String defaultWarehouseLocation(final TableIdentifier tableIdentifier) {
            throw new MetacatNotSupportedException("not supported");
        }

        @Override
        public boolean dropTable(final TableIdentifier identifier,
                                 final boolean purge) {
            throw new MetacatNotSupportedException("not supported");
        }

        @Override
        public void renameTable(final TableIdentifier from,
                                final TableIdentifier to) {
            throw new MetacatNotSupportedException("not supported");
        }

        /**
         * Creates a new instance of IcebergTableOps for the given table location, if not exists.
         *
         * @return a MetacatServerOps for the table
         */
        public IcebergTableOps getTableOps() {
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
        private String location;

        IcebergTableOps(final String location) {
            this.location = location;
            refresh();
        }

        @Override
        public FileIO io() {
            return new HadoopFileIO(conf);
        }

        @Override
        public TableMetadata refresh() {
            refreshFromMetadataLocation(this.location,
                connectorContext.getConfig().getIcebergRefreshFromMetadataLocationRetryNumber());
            return current();
        }

        @Override
        public String currentMetadataLocation() {
            return location;
        }

        @Override
        public TableMetadata current() {
            return super.current();
        }

        @Override
        public void commit(final TableMetadata base, final TableMetadata metadata) {
            if (!base.equals(metadata)) {
                location = writeNewMetadata(metadata, currentVersion() + 1);
                this.requestRefresh();
            }
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
     *
     * @param metricName metric name
     * @param tableName  table name of the operation
     */
    private void increaseCounter(final String metricName, final QualifiedName tableName) {
        this.registry.counter(registry.createId(metricName).withTags(tableName.parts())).increment();
    }
}
