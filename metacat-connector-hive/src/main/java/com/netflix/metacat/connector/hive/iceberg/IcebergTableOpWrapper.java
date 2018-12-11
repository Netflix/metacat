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

import com.netflix.iceberg.ScanSummary;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import lombok.AllArgsConstructor;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Iceberg table operation wrapper.
 *
 * @author zhenl
 * @since 1.2.0
 */
@AllArgsConstructor
public class IcebergTableOpWrapper {
    private final ConnectorContext connectorContext;

    /**
     * get iceberg partition map.
     *
     * @param icebergTable iceberg table
     * @param filter       iceberg filter expression
     * @return scan summary map
     */
    public Map<String, ScanSummary.PartitionMetrics> getPartitionMetricsMap(final Table icebergTable,
                                                                     @Nullable final Expression filter) {
        return (filter != null)
            ? ScanSummary.of(icebergTable.newScan().filter(filter))
                .limit(connectorContext.getConfig().getMaxPartitionsThreshold())
                .throwIfLimited()
                .build()
            :
            ScanSummary.of(icebergTable.newScan())  //the top x records
                .limit(connectorContext.getConfig().getIcebergTableSummaryFetchSize())
                .build();
    }
}
