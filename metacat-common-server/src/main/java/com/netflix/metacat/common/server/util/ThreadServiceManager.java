/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.common.server.util;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.netflix.metacat.common.server.Config;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.CompositeMonitor;
import com.netflix.servo.monitor.Monitors;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Thread service manager.
 * @author amajumdar
 */
@Slf4j
public class ThreadServiceManager {
    private ListeningExecutorService executor;
    private final int maxThreads;
    private final int maxQueueSize;
    private final String usage;

    /**
     * Constructor taking in the config.
     * @param config configurations
     */
    @Inject
    public ThreadServiceManager(final Config config) {
        this.maxThreads = config.getServiceMaxNumberOfThreads();
        this.maxQueueSize = 1000;
        this.usage = "service";
    }

    /**
     * Constructor.
     * @param maxThreads maximum number of threads
     * @param maxQueueSize maximum queue size
     * @param usage an identifier where this pool is used
     */
    public ThreadServiceManager(final int maxThreads, final int maxQueueSize, final String usage) {
        this.maxThreads = maxThreads;
        this.maxQueueSize = maxQueueSize;
        this.usage = usage;
    }

    /**
     * Starts the manager.
     */
    @PostConstruct
    public void start() {
        final ExecutorService executorService = newFixedThreadPool(maxThreads,
            "metacat-" + usage + "-pool-%d", maxQueueSize);
        executor = MoreExecutors.listeningDecorator(executorService);
        final CompositeMonitor<?> newThreadPoolMonitor =
            Monitors.newThreadPoolMonitor("metacat." + usage + ".pool", (ThreadPoolExecutor) executor);
        DefaultMonitorRegistry.getInstance().register(newThreadPoolMonitor);
    }

    @SuppressWarnings("checkstyle:hiddenfield")
    private ExecutorService newFixedThreadPool(final int nThreads, final String threadFactoryName,
        final int queueSize) {
        return new ThreadPoolExecutor(nThreads, nThreads,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueSize),
            new ThreadFactoryBuilder()
                .setNameFormat(threadFactoryName)
                .build(),
            (r, executor) -> {
                // this will block if the queue is full
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);

                }
            });
    }

    public ListeningExecutorService getExecutor() {
        return executor;
    }

    /**
     * Stops this manager.
     */
    @PreDestroy
    public void stop() {
        if (executor != null) {
            // Make the executor accept no new threads and finish all existing
            // threads in the queue
            executor.shutdown();
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                log.error("Error while shutting down executor service : ", e);
            }
        }
    }
}
