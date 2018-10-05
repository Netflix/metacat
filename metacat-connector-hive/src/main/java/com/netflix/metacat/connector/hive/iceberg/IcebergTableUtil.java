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
import com.netflix.iceberg.BaseMetastoreTableOperations;
import com.netflix.iceberg.BaseMetastoreTables;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.ScanSummary;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableMetadata;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.model.PartitionListRequest;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Iceberg Table Util.
 *
 * @author zhenl
 * @since 1.2.0
 */
public class IcebergTableUtil {
    private final Configuration conf;
    private final ConnectorContext connectorContext;

    /**
     * Constructor.
     *
     * @param connectorContext connector context
     */
    public IcebergTableUtil(final ConnectorContext connectorContext) {
        this.conf = new Configuration();
        this.connectorContext = connectorContext;
        connectorContext.getConfiguration().keySet()
            .forEach(key -> conf.set(key, connectorContext.getConfiguration().get(key)));
    }

    /**
     * get Partition Map.
     *
     * @param icebergTable      iceberg Table
     * @param partitionsRequest partitionsRequest
     * @return partition map
     */
    public Map<String, ScanSummary.PartitionMetrics> getIcebergTablePartitionMap(
        final Table icebergTable,
        final PartitionListRequest partitionsRequest) {
        return (partitionsRequest.getPageable() != null)
            ? ScanSummary.of(icebergTable.newScan()).build() //whole table scan with paging
            :
            ScanSummary.of(icebergTable.newScan())  //the top x records
                .limit(connectorContext.getConfig().getIcebergTableSummaryFetchSize())
                .build();
    }

    /**
     * get iceberg table.
     *
     * @param tableName             table name
     * @param tableMetadataLocation table metadata location
     * @return iceberg table
     */
    public Table getIcebergTable(final QualifiedName tableName, final String tableMetadataLocation) {
        return new IcebergMetastoreTables(tableMetadataLocation).load(tableName.toString());
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
        root.set(DataMetricConstants.DATA_METADATA_METRIC_NAME, getMetricValueNode(metrics));
        return root;
    }

    private ObjectNode getMetricValueNode(final ScanSummary.PartitionMetrics metrics) {
        final ObjectNode node = JsonNodeFactory.instance.objectNode();

        ObjectNode valueNode = JsonNodeFactory.instance.objectNode();
        valueNode.put(DataMetricConstants.DATA_METADATA_VALUE, metrics.recordCount());
        node.set(DataMetrics.rowCount.getMetricName(), valueNode);

        valueNode = JsonNodeFactory.instance.objectNode();
        valueNode.put(DataMetricConstants.DATA_METADATA_VALUE, metrics.fileCount());
        node.set(DataMetrics.fileCount.getMetricName(), valueNode);
        return node;
    }

    /**
     * Implemented BaseMetastoreTables to interact with iceberg library.
     * Load an iceberg table from a location.
     */
    private final class IcebergMetastoreTables extends BaseMetastoreTables {
        private final String tableLocation;

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
            return newIcebergTableOps(config);
        }

        /**
         * Creates a new instance of IcebergTableOps for the given table location.
         *
         * @param config a Configuration
         * @return a new MetacatServerOps for the table
         */
        private IcebergTableOps newIcebergTableOps(final Configuration config) {
            return new IcebergTableOps(tableLocation);
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
            refreshFromMetadataLocation(this.location);
            return current();
        }

        /**
         * No-op operation.
         */
        @Override
        public void commit(final TableMetadata base, final TableMetadata metadata) {
        }
    }

}
