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

import com.netflix.metacat.common.server.monitoring.LogConstants;
import com.netflix.spectator.api.Registry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.ApplicationEventMulticaster;

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

    private final ApplicationEventPublisher eventPublisher;
    private final ApplicationEventMulticaster eventMulticaster;
    private final Registry registry;

    /**
     * Constructor.
     *
     * @param eventPublisher   The synchronous event publisher to use
     * @param eventMulticaster The asynchronous event multicaster to use
     * @param registry         The registry to spectator
     */
    public MetacatEventBus(
            @Nonnull @NonNull final ApplicationEventPublisher eventPublisher,
            @Nonnull @NonNull final ApplicationEventMulticaster eventMulticaster,
            @Nonnull @NonNull final Registry registry
    ) {
        this.eventPublisher = eventPublisher;
        this.eventMulticaster = eventMulticaster;
        this.registry = registry;
    }

    /**
     * Post event asynchronously.
     *
     * @param event event
     */
    public void postAsync(final ApplicationEvent event) {
        log.debug("Received request to post an event {} asynchronously", event);
        registry.counter(LogConstants.CounterEventAsync.name()).increment();
        this.eventMulticaster.multicastEvent(event);
    }

    /**
     * Post event synchronously.
     *
     * @param event event
     */
    public void postSync(final ApplicationEvent event) {
        log.debug("Received request to post an event {} synchronously", event);
        registry.counter(LogConstants.CounterEventSync.name()).increment();
        this.eventPublisher.publishEvent(event);
    }

    /**
     * Registers an object.
     *
     * @param object object
     */
    public void register(final Object object) {
    }

    /**
     * De-registers an object.
     *
     * @param object object
     */
    public void unregister(final Object object) {
    }
}
