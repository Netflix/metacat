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

import com.netflix.metacat.common.QualifiedName;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Iceberg Table Request Metrics.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Slf4j
public class IcebergTableRequestMetrics {
    private final HashMap<String, Timer> timerMap = new HashMap<>();
    private final Registry registry;

    /**
     * Constructor.
     *
     * @param registry the spectator registry
     */
    public IcebergTableRequestMetrics(final Registry registry) {
        this.registry = registry;
        timerMap.put(IcebergRequestMetrics.TagLoadTable.getMetricName(), createTimer(
            IcebergRequestMetrics.TagLoadTable.getMetricName()));
        timerMap.put(IcebergRequestMetrics.TagGetPartitionMap.getMetricName(), createTimer(
            IcebergRequestMetrics.TagGetPartitionMap.getMetricName()));
    }

    private Timer createTimer(final String requestTag) {
        final HashMap<String, String> tags = new HashMap<>();
        tags.put("request", requestTag);
        return registry.timer(
            registry.createId(IcebergRequestMetrics.TimerIcebergRequest.getMetricName()).withTags(tags));
    }

    /**
     * record the duration to timer.
     *
     * @param metricName metric name.
     * @param duration   duration of the operation.
     */
    public void recordTimer(final String metricName, final long duration) {
        if (this.timerMap.containsKey(metricName)) {
            log.debug("## Time taken to complete {} is {} ms", metricName, duration);
            this.timerMap.get(metricName).record(duration, TimeUnit.MILLISECONDS);
        } else {
            log.error("Not supported metric {}", metricName);
        }
    }

    /**
     * increase the counter of operation.
     * @param metricName metric name
     * @param tableName table name of the operation
     */
    public void increaseCounter(final String metricName, final QualifiedName tableName) {
        this.registry.counter(registry.createId(metricName).withTags(tableName.parts())).increment();
    }
}
