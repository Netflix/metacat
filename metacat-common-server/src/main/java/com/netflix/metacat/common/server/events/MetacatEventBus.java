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
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
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
    private final Counter eventAsyncCounter;
    private final Counter eventSyncCounter;
    /**
     * Constructor.
     *
     * @param applicationEventMulticaster The event multicaster to use
     * @param registry         The registry to spectator
     */
    public MetacatEventBus(
        @Nonnull @NonNull final MetacatApplicationEventMulticaster applicationEventMulticaster,
        @Nonnull @NonNull final Registry registry
    ) {
        this.applicationEventMulticaster = applicationEventMulticaster;
        this.eventAsyncCounter = registry.counter(Metrics.CounterEventAsync.getMetricName());
        this.eventSyncCounter = registry.counter(Metrics.CounterEventSync.getMetricName());
    }

    /**
     * Post event asynchronously.
     *
     * @param event event
     */
    public void postAsync(final ApplicationEvent event) {
        log.debug("Received request to post an event {} asynchronously", event);
        this.eventAsyncCounter.increment();
        this.applicationEventMulticaster.postAsync(event);
    }

    /**
     * Post event synchronously.
     *
     * @param event event
     */
    public void postSync(final ApplicationEvent event) {
        log.debug("Received request to post an event {} synchronously", event);
        this.eventSyncCounter.increment();
        this.applicationEventMulticaster.postSync(event);
    }
}
