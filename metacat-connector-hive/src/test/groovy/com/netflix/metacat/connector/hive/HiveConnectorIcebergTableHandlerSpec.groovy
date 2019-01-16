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

package com.netflix.metacat.connector.hive

import com.google.common.collect.ImmutableMap
import com.netflix.iceberg.ScanSummary
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.exception.MetacatException
import com.netflix.metacat.common.exception.MetacatNotSupportedException
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.connector.hive.iceberg.DataMetadataMetricConstants
import com.netflix.metacat.connector.hive.iceberg.DataMetadataMetrics
import com.netflix.metacat.connector.hive.iceberg.IcebergTableCriteria
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler
import com.netflix.metacat.connector.hive.util.HiveConfigConstants
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Shared
import spock.lang.Specification

class HiveConnectorIcebergTableHandlerSpec extends Specification{
    @Shared
    ConnectorContext connectorContext = new ConnectorContext(
        "testHive",
        "testHive",
        "hive",
        Mock(Config),
        new SimpleMeterRegistry(),
        ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true")
    )

    def "Test for populate data metadata" () {
        def icebergHandler = new IcebergTableHandler(connectorContext)

        def metrics = Mock(ScanSummary.PartitionMetrics)
        when:
        def node = icebergHandler.getDataMetadataFromIcebergMetrics(metrics)

        then:
        1 * metrics.fileCount() >> 1
        1 * metrics.recordCount() >> 2
        node != null
        node.get(DataMetadataMetricConstants.DATA_METADATA_METRIC_NAME)
            .get(DataMetadataMetrics.rowCount.metricName).get(DataMetadataMetricConstants.DATA_METADATA_VALUE).asInt() == 2
        node.get(DataMetadataMetricConstants.DATA_METADATA_METRIC_NAME)
            .get(DataMetadataMetrics.fileCount.metricName).get(DataMetadataMetricConstants.DATA_METADATA_VALUE).asInt() == 1
    }

    def "Test for getIcebergTable with criteria throw exception" () {
        def criteriaImpl = Mock(IcebergTableCriteria) {
           checkCriteria(_,_) >> {
                throw new MetacatException("can't load table",
                    new MetacatNotSupportedException("manifest too larg"))
            }
        }
        def icebergHandler = new IcebergTableHandler(connectorContext)
        icebergHandler.icebergTableCriteria = criteriaImpl
        when:
        ret = icebergHandler.getIcebergTable(QualifiedName.fromString("testing/db/table"), "abc")
        then:
        thrown(MetacatException)
    }

}
