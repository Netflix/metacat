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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.server.monitoring.CounterWrapper;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.CompositeMonitor;
import com.netflix.servo.monitor.Monitors;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Event bus.
 */
@Slf4j
public class MetacatEventBus {
    private final Map<String, EventBus> asyncEventBusMap;
    private final EventBus syncEventBus;
    private final List<ExecutorService> executors;
    private final Config config;

    /**
     * Constructor.
     *
     * @param config config
     */
    @Inject
    public MetacatEventBus(final Config config) {
        this.config = config;
        this.syncEventBus = new EventBus("metacat-sync-event-bus");
        this.asyncEventBusMap = Maps.newHashMap();
        this.executors = Lists.newArrayList();
    }

    /**
     * Post event asynchronously.
     *
     * @param event event
     */
    public void postAsync(final Object event) {
        log.debug("Received request to post an event {} asynchronously", event);
        CounterWrapper.incrementCounter("metacat.events.async");
        this.asyncEventBusMap.values().forEach(eventBus -> eventBus.post(event));
    }

    /**
     * Post event synchronously.
     *
     * @param event event
     */
    public void postSync(final Object event) {
        log.debug("Received request to post an event {} synchronously", event);
        CounterWrapper.incrementCounter("metacat.events.sync");
        this.syncEventBus.post(event);
    }

    /**
     * Registers an object.
     *
     * @param object object
     * @param identifier identifier of the async event bus. If an event bus with the given identifier does not exist,
     *                   it is created.
     */
    public void register(final Object object, final String identifier) {
        getAsyncEventBus(identifier).register(object);
        syncEventBus.register(object);
    }

    private EventBus getAsyncEventBus(final String identifier) {
        EventBus result = asyncEventBusMap.get(identifier);
        if (result == null) {
            final ExecutorService executor = Executors.newFixedThreadPool(config.getEventBusThreadCount(),
                new ThreadFactoryBuilder().setNameFormat("metacat-" + identifier + "-event-pool-%d").build());
            final CompositeMonitor<?> newThreadPoolMonitor =
                Monitors.newThreadPoolMonitor("metacat." + identifier + ".event.pool", (ThreadPoolExecutor) executor);
            DefaultMonitorRegistry.getInstance().register(newThreadPoolMonitor);
            executors.add(executor);
            result = new AsyncEventBus("metacat-" + identifier + "-async-event-bus", executor);
            asyncEventBusMap.put(identifier, result);
        }
        return result;
    }

    /**
     * Shut down the executor. Taken from the javadoc for {@code ExecutorService}
     *
     * @see ExecutorService
     */
    @PreDestroy
    public void shutdown() {
        this.executors.forEach(executor -> {
            executor.shutdown();
            try {
                // Wait a while for existing tasks to terminate
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                        System.err.println("Event bus async thread executor did not terminate");
                    }
                }
            } catch (final InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                executor.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        });
    }
}
