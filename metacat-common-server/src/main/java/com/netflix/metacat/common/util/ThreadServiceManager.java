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

package com.netflix.metacat.common.util;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.netflix.metacat.common.server.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by amajumdar on 4/4/16.
 */
public class ThreadServiceManager {
    private static final Logger log = LoggerFactory.getLogger(ThreadServiceManager.class);
    private ListeningExecutorService executor = null;
    @Inject
    Config config;

    @PostConstruct
    public void start() {
        ExecutorService executorService = newFixedThreadPool( config.getServiceMaxNumberOfThreads(), "metacat-service-pool-%d",  1000);
        executor = MoreExecutors.listeningDecorator(executorService) ;
    }

    private ExecutorService newFixedThreadPool(int nThreads, String threadFactoryName, int queueSize) {
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

    public ListeningExecutorService getExecutor(){
        return executor;
    }

    @PreDestroy
    public void stop(){
        if(executor != null){
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
