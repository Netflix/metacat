/*
 *
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
 *
 */
package com.netflix.metacat.main.services.notifications.sns;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.notifications.sns.SNSMessage;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * SNS Notification Metric.
 *
 * @author zhenl
 * @since 1.1.0
 */

@Slf4j
@Getter
public class SNSNotificationMetric {
    private final Registry registry;
    private final HashMap<String, Counter> counterHashMap = new HashMap<>();

    /**
     * Constructor.
     *
     * @param registry The registry handle of spectator
     */
    public SNSNotificationMetric(
        final Registry registry
    ) {
        this.registry = registry;
        this.counterHashMap.put(Metrics.CounterSNSNotificationTablePartitionAdd.getMetricName(),
            registry.counter(registry.createId(Metrics.CounterSNSNotificationTablePartitionAdd.getMetricName())
                .withTags(Metrics.statusSuccessMap)));
        this.counterHashMap.put(Metrics.CounterSNSNotificationPartitionDelete.getMetricName(),
            registry.counter(registry.createId(Metrics.CounterSNSNotificationPartitionDelete.getMetricName())
                .withTags(Metrics.statusSuccessMap)));
        this.counterHashMap.put(Metrics.CounterSNSNotificationTableCreate.getMetricName(),
            registry.counter(registry.createId(Metrics.CounterSNSNotificationTableCreate.getMetricName())
                .withTags(Metrics.statusSuccessMap)));
        this.counterHashMap.put(Metrics.CounterSNSNotificationTableDelete.getMetricName(),
            registry.counter(registry.createId(Metrics.CounterSNSNotificationTableDelete.getMetricName())
                .withTags(Metrics.statusSuccessMap)));
        this.counterHashMap.put(Metrics.CounterSNSNotificationTableRename.getMetricName(),
            registry.counter(registry.createId(Metrics.CounterSNSNotificationTableRename.getMetricName())
                .withTags(Metrics.statusSuccessMap)));
        this.counterHashMap.put(Metrics.CounterSNSNotificationTableUpdate.getMetricName(),
            registry.counter(registry.createId(Metrics.CounterSNSNotificationTableUpdate.getMetricName())
                .withTags(Metrics.statusSuccessMap)));
        this.counterHashMap.put(Metrics.CounterSNSNotificationPublishMessageSizeExceeded.getMetricName(),
            registry.counter(
                registry.createId(Metrics.CounterSNSNotificationPublishMessageSizeExceeded.getMetricName())));
    }

    void counterIncrement(final String counterKey) {
        if (counterHashMap.containsKey(counterKey)) {
            this.counterHashMap.get(counterKey).increment();
        } else {
            log.error("SNS Notification does not suport counter for " + counterKey);
        }
    }

    void handleException(
        final QualifiedName name,
        final String message,
        final String counterKey,
        @Nullable final SNSMessage payload,
        final Exception e
    ) {
        log.error("{} with payload: {}", message, payload, e);
        final Map<String, String> tags = new HashMap<>(name.parts());
        tags.putAll(Metrics.statusFailureMap);
        this.registry.counter(this.registry.createId(counterKey).withTags(tags)).increment();
    }

    void recordTime(final SNSMessage<?> message, final String timeName) {
        final Timer timer = this.registry.timer(
            timeName,
            Metrics.TagEventsType.getMetricName(),
            message.getClass().getName()
        );
        timer.record(this.registry.clock().wallTime() - message.getTimestamp(), TimeUnit.MILLISECONDS);
    }
}
