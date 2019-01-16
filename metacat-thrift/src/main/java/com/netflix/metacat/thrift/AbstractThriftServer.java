/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.metacat.thrift;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.util.RegistryUtil;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base implementation for thrift server.
 */
@Slf4j
public abstract class AbstractThriftServer {
    protected final Config config;
    protected final MeterRegistry registry;
    @Getter
    private final int portNumber;
    private final String threadPoolNameFormat;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicInteger serverThreadCount = new AtomicInteger(0);
    @Getter
    private TServer server;

    protected AbstractThriftServer(
        @NonNull final Config config,
        @NonNull final MeterRegistry registry,
        final int portNumber,
        @NonNull final String threadPoolNameFormat
    ) {
        this.config = config;
        this.registry = registry;
        this.portNumber = portNumber;
        this.threadPoolNameFormat = threadPoolNameFormat;
    }

    /**
     * Returns the thrift processor.
     *
     * @return thrift processor
     */
    public abstract TProcessor getProcessor();

    /**
     * Returns the server event handler.
     *
     * @return server event handler
     */
    public abstract TServerEventHandler getServerEventHandler();

    /**
     * Returns the server name.
     *
     * @return server name
     */
    public abstract String getServerName();

    /**
     * Returns true, if the server event handler exists.
     *
     * @return true, if the server event handler exists
     */
    public abstract boolean hasServerEventHandler();

    /**
     * Server initialization.
     *
     * @throws Exception error
     */
    public void start() throws Exception {
        log.info("initializing thrift server {}", getServerName());
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat(threadPoolNameFormat)
            .setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread: {}", t.getName(), e))
            .build();
        final ExecutorService executorService = new ThreadPoolExecutor(
            Math.min(2, config.getThriftServerMaxWorkerThreads()),
            config.getThriftServerMaxWorkerThreads(),
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            threadFactory
        );
        RegistryUtil.registerThreadPool(registry, threadPoolNameFormat, (ThreadPoolExecutor) executorService);
        final int timeout = config.getThriftServerSocketClientTimeoutInSeconds() * 1000;
        final TServerTransport serverTransport = new TServerSocket(portNumber, timeout);
        startServing(executorService, serverTransport);
    }

    private void startServing(final ExecutorService executorService, final TServerTransport serverTransport) {
        if (!stopping.get()) {
            final TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
                .processor(getProcessor())
                .executorService(executorService);
            server = new TThreadPoolServer(serverArgs);
            if (hasServerEventHandler()) {
                server.setServerEventHandler(getServerEventHandler());
            }

            final String threadName = getServerName() + "-thread-#" + serverThreadCount.incrementAndGet();
            new Thread(threadName) {
                @Override
                public void run() {
                    log.debug("starting serving");
                    try {
                        server.serve();
                    } catch (Throwable t) {
                        if (!stopping.get()) {
                            log.error("Unexpected exception in {}. This probably "
                                + "means that the worker pool was exhausted. "
                                + "Increase 'metacat.thrift.server_max_worker_threads' "
                                + "from {} or throttle the number of requests. "
                                + "This server thread is not in a bad state so starting a new one.",
                                getServerName(), config.getThriftServerMaxWorkerThreads(), t);
                            startServing(executorService, serverTransport);
                        } else {
                            log.debug("stopping serving");
                        }
                    }
                    log.debug("started serving");
                }
            }.start();
        }
    }

    /**
     * Server shutdown.
     *
     * @throws Exception error
     */
    public void stop() throws Exception {
        log.info("stopping thrift server {}", getServerName());
        if (stopping.compareAndSet(false, true) && server != null) {
            log.debug("stopping serving");
            server.stop();
            log.debug("stopped serving");
        }
    }
}
