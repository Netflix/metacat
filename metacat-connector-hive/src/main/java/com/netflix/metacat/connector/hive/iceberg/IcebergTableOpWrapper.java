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

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.netflix.iceberg.ScanSummary;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Iceberg table operation wrapper.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Slf4j
public class IcebergTableOpWrapper {
    private final Config config;
    private final Map<String, String> configuration;
    private final ThreadServiceManager threadServiceManager;

    /**
     * Constructor.
     * @param connectorContext      server context
     * @param threadServiceManager  executor service
     */
    public IcebergTableOpWrapper(final ConnectorContext connectorContext,
                                 final ThreadServiceManager threadServiceManager) {
        this.config = connectorContext.getConfig();
        this.configuration = connectorContext.getConfiguration();
        this.threadServiceManager = threadServiceManager;
    }

    /**
     * get iceberg partition map.
     *
     * @param icebergTable iceberg table
     * @param filter       iceberg filter expression
     * @return scan summary map
     */
    public Map<String, ScanSummary.PartitionMetrics> getPartitionMetricsMap(final Table icebergTable,
                                                                     @Nullable final Expression filter) {
        Map<String, ScanSummary.PartitionMetrics> result = Maps.newHashMap();
        //
        // Cancel the iceberg call if it times out.
        //
        final Future<Map<String, ScanSummary.PartitionMetrics>> future = threadServiceManager.getExecutor()
            .submit(() -> (filter != null) ? ScanSummary.of(icebergTable.newScan().filter(filter))
            .limit(config.getMaxPartitionsThreshold())
            .throwIfLimited()
            .build()
            :
            ScanSummary.of(icebergTable.newScan())  //the top x records
                .limit(config.getIcebergTableSummaryFetchSize())
                .build());
        try {
            final int getIcebergPartitionsTimeout = Integer.parseInt(configuration
                .getOrDefault(HiveConfigConstants.GET_ICEBERG_PARTITIONS_TIMEOUT, "120"));
            result = future.get(getIcebergPartitionsTimeout, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            try {
                future.cancel(true);
            } catch (Exception ignored) {
                log.warn("Failed cancelling the task that gets the partitions for an iceberg table.");
            }
            Throwables.propagate(e);
        }
        return result;
    }
}
