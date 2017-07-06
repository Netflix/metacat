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
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;


/**
 * Hive Connector Fast Service Metric.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Getter
@Slf4j
public class HiveConnectorFastServiceMetric {
    private final HashMap<String, Timer> timerMap = new HashMap<>();
    private final Counter getHiveTablePartsFailureCounter;

    /**
     * Constructor.
     *
     * @param registry the spectator registry
     */
    public HiveConnectorFastServiceMetric(final Registry registry) {
        timerMap.put(HiveMetrics.getPartitionCount.getMetricName(), createTimer(registry,
            HiveMetrics.getPartitionCount.getMetricName()));
        timerMap.put(HiveMetrics.getPartitions.getMetricName(), createTimer(registry,
            HiveMetrics.getPartitions.getMetricName()));
        timerMap.put(HiveMetrics.getPartitionKeys.getMetricName(), createTimer(registry,
            HiveMetrics.getPartitionKeys.getMetricName()));
        timerMap.put(HiveMetrics.getPartitionNames.getMetricName(), createTimer(registry,
            HiveMetrics.getPartitionNames.getMetricName()));
        timerMap.put(HiveMetrics.tableExists.getMetricName(), createTimer(registry,
            HiveMetrics.tableExists.getMetricName()));
        timerMap.put(HiveMetrics.getPartitionNames.getMetricName(), createTimer(registry,
            HiveMetrics.getTableNames.getMetricName()));

        getHiveTablePartsFailureCounter = registry.counter(
            HiveMetrics.CounterHiveExperimentGetTablePartitionsFailure.getMetricName());

    }

    private Timer createTimer(final Registry registry, final String requestTag) {
        final HashMap<String, String> tags = new HashMap<>();
        tags.put("request", requestTag);
        return registry.timer(registry.createId(HiveMetrics.TimerFastHiveRequest.getMetricName()).withTags(tags));
    }

    /**
     * record the duration to timer.
     *
     * @param metricName metric name.
     * @param duration   duration of the operation.
     */
    public void recordTimer(final String metricName, final long duration) {
        if (this.timerMap.containsKey(metricName)) {
            log.debug("### Time taken to complete {} is {} ms", metricName, duration);
            this.timerMap.get(metricName).record(duration, TimeUnit.MILLISECONDS);
        } else {
            log.error("Not supported metric {}", metricName);
        }
    }
}
