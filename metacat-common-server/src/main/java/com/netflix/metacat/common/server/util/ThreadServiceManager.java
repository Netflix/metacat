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
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.spectator.api.Registry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Thread service manager.
 *
 * @author amajumdar
 */
@Getter
@Slf4j
public class ThreadServiceManager {
    private  ListeningExecutorService executor;

    /**
     * Constructor.
     *
     * @param registry registry for spectator
     * @param config Program configuration
     */
    @Autowired
    public ThreadServiceManager(final Registry registry, final Config config) {
        final ExecutorService executorService = newFixedThreadPool(
            config.getServiceMaxNumberOfThreads(),
            config.getServiceMaxNumberOfThreads(),
            "metacat-service-pool-%d",
            1000
        );
        this.executor = MoreExecutors.listeningDecorator(executorService);
        RegistryUtil.registerThreadPool(registry, "metacat-service-pool", (ThreadPoolExecutor) executorService);
    }

    /**
     * Constructor.
     *
     * @param registry registry for spectator
     * @param maxThreads maximum number of threads
     * @param maxQueueSize maximum queue size
     * @param usage an identifier where this pool is used
     */
    public ThreadServiceManager(final Registry registry, final int maxThreads,
        final int maxQueueSize, final String usage) {
        final ExecutorService executorService = newFixedThreadPool(
            2,
            maxThreads,
            "metacat-" + usage + "-pool-%d",
            maxQueueSize
        );
        this.executor = MoreExecutors.listeningDecorator(executorService);
        RegistryUtil.registerThreadPool(registry, "metacat-" + usage + "-pool",
            (ThreadPoolExecutor) executorService);
    }

    /**
     * Stops this manager.
     */
    @PreDestroy
    public void stop() {
        if (this.executor != null) {
            // Make the executor accept no new threads and finish all existing
            // threads in the queue
            this.executor.shutdown();
            try {
                this.executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                log.error("Error while shutting down executor service : ", e);
            }
        }
    }

    @SuppressWarnings("checkstyle:hiddenfield")
    private ExecutorService newFixedThreadPool(
        final int minThreads,
        final int maxThreads,
        final String threadFactoryName,
        final int queueSize
    ) {
        return new ThreadPoolExecutor(
            minThreads,
            maxThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueSize),
            new ThreadFactoryBuilder().setNameFormat(threadFactoryName).build(),
            (r, executor) -> {
                // this will block if the queue is full
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    throw Throwables.propagate(e);

                }
            }
        );
    }
}
