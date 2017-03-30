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

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.netflix.metacat.common.monitoring.CounterWrapper;
import com.netflix.metacat.common.server.Config;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Event bus.
 */
@Slf4j
public class MetacatEventBus {
    private final AsyncEventBus asyncEventBus;
    private final EventBus syncEventBus;
    private final ExecutorService executor;

    /**
     * Constructor.
     *
     * @param config config
     */
    @Inject
    public MetacatEventBus(final Config config) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("metacat-event-pool-%d").build();
        final int threadCount = config.getEventBusThreadCount();
        this.executor = Executors.newFixedThreadPool(threadCount, threadFactory);
        this.asyncEventBus = new AsyncEventBus(
            "metacat-async-event-bus",
            this.executor
        );
        this.syncEventBus = new EventBus("metacat-sync-event-bus");
    }

    /**
     * Post event asynchronously.
     *
     * @param event event
     */
    public void postAsync(final Object event) {
        log.debug("Received request to post an event {} asynchronously", event);
        CounterWrapper.incrementCounter("metacat.events.async");
        this.asyncEventBus.post(event);
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
     */
    public void register(final Object object) {
        asyncEventBus.register(object);
        syncEventBus.register(object);
    }

    /**
     * De-registers an object.
     *
     * @param object object
     */
    public void unregister(final Object object) {
        asyncEventBus.unregister(object);
        syncEventBus.unregister(object);
    }

    /**
     * Shut down the executor. Taken from the javadoc for {@code ExecutorService}
     *
     * @see ExecutorService
     */
    @PreDestroy
    public void shutdown() {
        this.executor.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!this.executor.awaitTermination(60, TimeUnit.SECONDS)) {
                this.executor.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!this.executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.err.println("Event bus async thread executor did not terminate");
                }
            }
        } catch (final InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            this.executor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
