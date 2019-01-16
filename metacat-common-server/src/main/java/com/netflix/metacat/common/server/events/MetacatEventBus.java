/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.common.server.events;

import com.netflix.metacat.common.server.monitoring.Metrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEvent;

import javax.annotation.Nonnull;

/**
 * Event bus.
 *
 * @author amajumdar
 * @author tgianos
 * @since 0.x
 */
@Slf4j
public class MetacatEventBus {

    private final MetacatApplicationEventMulticaster applicationEventMulticaster;
    private final Counter eventPublishCounter;

    /**
     * Constructor.
     *
     * @param applicationEventMulticaster The event multicaster to use
     * @param registry                    The registry to micrometer
     */
    public MetacatEventBus(
        @Nonnull @NonNull final MetacatApplicationEventMulticaster applicationEventMulticaster,
        @Nonnull @NonNull final MeterRegistry registry
    ) {
        this.applicationEventMulticaster = applicationEventMulticaster;
        this.eventPublishCounter = registry.counter(Metrics.CounterEventPublish.getMetricName());
    }

    /**
     * Post event.
     *
     * @param event event
     */
    public void post(final ApplicationEvent event) {
        log.debug("Received request to post an event {}", event);
        this.eventPublishCounter.increment();
        this.applicationEventMulticaster.post(event);
    }
}
