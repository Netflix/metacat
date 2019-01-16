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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
    private static final String REQUEST_TAG = "request";
    private final HashMap<String, Timer> timerMap = new HashMap<>();
    private final Counter getHiveTablePartsFailureCounter;

    /**
     * Constructor.
     *
     * @param registry the micrometer registry
     */
    public HiveConnectorFastServiceMetric(final MeterRegistry registry) {
        timerMap.put(HiveMetrics.TagGetPartitionCount.getMetricName(), createTimer(registry,
            HiveMetrics.TagGetPartitionCount.getMetricName()));
        timerMap.put(HiveMetrics.TagGetPartitions.getMetricName(), createTimer(registry,
            HiveMetrics.TagGetPartitions.getMetricName()));
        timerMap.put(HiveMetrics.TagGetPartitionKeys.getMetricName(), createTimer(registry,
            HiveMetrics.TagGetPartitionKeys.getMetricName()));
        timerMap.put(HiveMetrics.TagGetPartitionNames.getMetricName(), createTimer(registry,
            HiveMetrics.TagGetPartitionNames.getMetricName()));
        timerMap.put(HiveMetrics.TagTableExists.getMetricName(), createTimer(registry,
            HiveMetrics.TagTableExists.getMetricName()));
        timerMap.put(HiveMetrics.TagGetTableNames.getMetricName(), createTimer(registry,
            HiveMetrics.TagGetTableNames.getMetricName()));
        timerMap.put(HiveMetrics.TagAddPartitions.getMetricName(), createTimer(registry,
            HiveMetrics.TagAddPartitions.getMetricName()));
        timerMap.put(HiveMetrics.TagAlterPartitions.getMetricName(), createTimer(registry,
            HiveMetrics.TagAlterPartitions.getMetricName()));
        timerMap.put(HiveMetrics.TagDropHivePartitions.getMetricName(), createTimer(registry,
            HiveMetrics.TagDropHivePartitions.getMetricName()));
        timerMap.put(HiveMetrics.TagAddDropPartitions.getMetricName(), createTimer(registry,
            HiveMetrics.TagAddDropPartitions.getMetricName()));

        getHiveTablePartsFailureCounter = registry.counter(
            HiveMetrics.CounterHiveExperimentGetTablePartitionsFailure.getMetricName());
    }

    private Timer createTimer(final MeterRegistry registry, final String requestTag) {
        return registry.timer(HiveMetrics.TimerFastHiveRequest.getMetricName(), "request", requestTag);
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
