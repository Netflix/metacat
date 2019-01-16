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

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.notifications.sns.SNSMessage;
import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.metacat.common.server.util.RegistryUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
    private final MeterRegistry registry;
    private final HashMap<String, Counter> counterHashMap = new HashMap<>();

    /**
     * Constructor.
     *
     * @param registry The registry handle of micrometer
     */
    public SNSNotificationMetric(
        final MeterRegistry registry
    ) {
        this.registry = registry;
        this.counterHashMap.put(Metrics.CounterSNSNotificationTablePartitionAdd.getMetricName(),
            registry.counter(Metrics.CounterSNSNotificationTablePartitionAdd.getMetricName(),
            Metrics.tagStatusSuccessSet));
        this.counterHashMap.put(Metrics.CounterSNSNotificationPartitionDelete.getMetricName(),
            registry.counter(Metrics.CounterSNSNotificationPartitionDelete.getMetricName(),
            Metrics.tagStatusSuccessSet));
        this.counterHashMap.put(Metrics.CounterSNSNotificationTableCreate.getMetricName(),
            registry.counter(Metrics.CounterSNSNotificationTableCreate.getMetricName(), Metrics.tagStatusSuccessSet));
        this.counterHashMap.put(Metrics.CounterSNSNotificationTableDelete.getMetricName(),
            registry.counter(Metrics.CounterSNSNotificationTableDelete.getMetricName(), Metrics.tagStatusSuccessSet));
        this.counterHashMap.put(Metrics.CounterSNSNotificationTableRename.getMetricName(),
            registry.counter(Metrics.CounterSNSNotificationTableRename.getMetricName(), Metrics.tagStatusSuccessSet));
        this.counterHashMap.put(Metrics.CounterSNSNotificationTableUpdate.getMetricName(),
            registry.counter(Metrics.CounterSNSNotificationTableUpdate.getMetricName(), Metrics.tagStatusSuccessSet));
        this.counterHashMap.put(Metrics.CounterSNSNotificationPublishMessageSizeExceeded.getMetricName(),
            registry.counter(Metrics.CounterSNSNotificationPublishMessageSizeExceeded.getMetricName()));
        this.counterHashMap.put(Metrics.CounterSNSNotificationPartitionAdd.getMetricName(),
            registry.counter(Metrics.CounterSNSNotificationPartitionAdd.getMetricName()));
        this.counterHashMap.put(Metrics.CounterSNSNotificationPublishFallback.getMetricName(),
            registry.counter(Metrics.CounterSNSNotificationPublishFallback.getMetricName()));
    }

    void counterIncrement(final String counterKey) {
        if (counterHashMap.containsKey(counterKey)) {
            this.counterHashMap.get(counterKey).increment();
        } else {
            log.error("SNS Notification does not support counter for {}", counterKey);
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
        final Set<Tag> tags = RegistryUtil.qualifiedNameToTagsSet(name);
        tags.addAll(Metrics.tagStatusFailureSet);
        this.registry.counter(counterKey, tags).increment();
        Throwables.propagate(e);
    }

    void recordTime(final SNSMessage<?> message, final String timeName) {
        final Timer timer = this.registry.timer(
            timeName,
            Metrics.TagEventsType.getMetricName(),
            message.getClass().getName()
        );
        timer.record(System.currentTimeMillis() - message.getTimestamp(), TimeUnit.MILLISECONDS);
    }

    void recordPartitionLatestDeleteColumn(final QualifiedName name,
                                           @Nullable final String latestDeleteColumn,
                                           final String message) {
        final Set<Tag> tags = Sets.newHashSet();
        for (Map.Entry<String, String> part : name.parts().entrySet()) {
            tags.add(Tag.of(part.getKey(), part.getValue()));
        }
        if (latestDeleteColumn != null) {
            tags.add(Tag.of("latestDeleteColumn", latestDeleteColumn));
        }
        tags.add(Tag.of("message", message));
        this.registry.counter(
            Metrics.CounterSNSNotificationPartitionLatestDeleteColumnAdd.getMetricName(),
            tags).increment();
    }
}
