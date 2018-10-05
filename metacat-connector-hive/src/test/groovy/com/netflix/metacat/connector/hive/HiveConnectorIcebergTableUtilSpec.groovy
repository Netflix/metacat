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
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.connector.hive.iceberg.DataMetricConstants
import com.netflix.metacat.connector.hive.iceberg.DataMetrics
import com.netflix.metacat.connector.hive.iceberg.IcebergTableUtil
import com.netflix.metacat.connector.hive.util.HiveConfigConstants
import com.netflix.spectator.api.Registry
import spock.lang.Shared
import spock.lang.Specification

class HiveConnectorIcebergTableUtilSpec extends Specification{
    @Shared
    ConnectorContext connectorContext = new ConnectorContext(
        "testHive",
        "testHive",
        "hive",
        Mock(Config),
        Mock(Registry),
        ImmutableMap.of(HiveConfigConstants.ALLOW_RENAME_TABLE, "true")
    )
    def "Test for populate data metadata" () {
        def icebergUtil = new IcebergTableUtil(connectorContext)
        def partionInfo = PartitionInfo.builder().build()
        def metrics = Mock(ScanSummary.PartitionMetrics)
        when:
        def node = icebergUtil.getDataMetadataFromIcebergMetrics(metrics)

        then:
        1 * metrics.fileCount() >> 1
        1 * metrics.recordCount() >> 2
        node != null
        node.get(DataMetricConstants.DATA_METADATA_METRIC_NAME)
            .get(DataMetrics.rowCount.metricName).get(DataMetricConstants.DATA_METADATA_VALUE).asInt() == 2
        node.get(DataMetricConstants.DATA_METADATA_METRIC_NAME)
            .get(DataMetrics.fileCount.metricName).get(DataMetricConstants.DATA_METADATA_VALUE).asInt() == 1
    }
}
