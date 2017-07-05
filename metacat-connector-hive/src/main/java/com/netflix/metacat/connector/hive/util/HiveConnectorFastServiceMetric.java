/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive.util;

import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.Getter;

import java.util.HashMap;


/**
 * Hive Connector Fast Service Metric.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Getter
public class HiveConnectorFastServiceMetric {
    private final Timer fastHivePartitionCountTimer;
    private final Timer fastHiveGetPartitionTimer;
    private final Timer fastHiveGetPartitionKeysTimer;
    private final Timer fastHiveGetPartitionNamesTimer;
    private final Timer fastHiveTableExistsTimer;
    private final Timer fastHiveTableGetTableNamesTimer;

    /**
     * Constructor.
     *
     * @param registry the spectator registry
     */
    public HiveConnectorFastServiceMetric(final Registry registry) {
        this.fastHivePartitionCountTimer = createTimer(registry,
            HiveMetrics.TimerFastHiveRequest.getMetricName(), HiveMetrics.getPartitionCount.getMetricName());
        this.fastHiveGetPartitionTimer = createTimer(registry,
            HiveMetrics.TimerFastHiveRequest.getMetricName(), HiveMetrics.getPartitions.getMetricName());
        this.fastHiveGetPartitionKeysTimer = createTimer(registry,
            HiveMetrics.TimerFastHiveRequest.getMetricName(), HiveMetrics.getPartitionKeys.getMetricName());
        this.fastHiveGetPartitionNamesTimer = createTimer(registry,
            HiveMetrics.TimerFastHiveRequest.getMetricName(), HiveMetrics.getPartitionNames.getMetricName());
        this.fastHiveTableExistsTimer = createTimer(registry,
            HiveMetrics.TimerFastHiveRequest.getMetricName(), HiveMetrics.exists.getMetricName());
        this.fastHiveTableGetTableNamesTimer = createTimer(registry,
            HiveMetrics.TimerFastHiveRequest.getMetricName(), HiveMetrics.getTableNames.getMetricName());
    }

    private Timer createTimer(final Registry registry, final String timerName, final String requestTag) {
        final HashMap<String, String> tags = new HashMap<>();
        tags.put("request", HiveMetrics.getPartitionCount.getMetricName());
        return registry.timer(
            registry.createId(HiveMetrics.TimerFastHiveRequest.getMetricName())
                .withTags(tags));
    }

}
