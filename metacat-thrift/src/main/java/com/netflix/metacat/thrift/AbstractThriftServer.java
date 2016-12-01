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
import com.netflix.metacat.common.server.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    protected final int portNumber;
    protected final String threadPoolNameFormat;
    protected final AtomicBoolean stopping = new AtomicBoolean(false);
    protected final AtomicInteger serverThreadCount = new AtomicInteger(0);
    protected TServer server;

    protected AbstractThriftServer(Config config, int portNumber, String threadPoolNameFormat) {
        this.config = config;
        this.portNumber = portNumber;
        this.threadPoolNameFormat = threadPoolNameFormat;
    }

    public abstract TProcessor getProcessor();

    public abstract TServerEventHandler getServerEventHandler();

    public abstract String getServerName();

    public abstract boolean hasServerEventHandler();

    public void start() throws Exception {
        log.info("initializing thrift server {}", getServerName());
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat(threadPoolNameFormat)
            .setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread: " + t.getName(), e))
            .build();
        ExecutorService executorService = new ThreadPoolExecutor(
            Math.min(2, config.getThriftServerMaxWorkerThreads()),
            config.getThriftServerMaxWorkerThreads(),
            60L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            threadFactory
        );
        int timeout = config.getThriftServerSocketClientTimeoutInSeconds() * 1000;
        TServerTransport serverTransport = new TServerSocket(portNumber, timeout);
        startServing(executorService, serverTransport);
    }

    private void startServing(ExecutorService executorService, TServerTransport serverTransport) {
        if (!stopping.get()) {
            TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
                .processor(getProcessor())
                .executorService(executorService);
            server = new TThreadPoolServer(serverArgs);
            if (hasServerEventHandler()) {
                server.setServerEventHandler(getServerEventHandler());
            }

            String threadName = getServerName() + "-thread-#" + serverThreadCount.incrementAndGet();
            new Thread(threadName) {
                @Override
                public void run() {
                    log.debug("starting serving");
                    try {
                        server.serve();
                    } catch (Throwable t) {
                        if (!stopping.get()) {
                            log.error("Unexpected exception in " + getServerName()
                                + ". This probably means that the worker "
                                + " pool was exhausted. Increase 'metacat.thrift.server_max_worker_threads' from "
                                + config.getThriftServerMaxWorkerThreads() + " or throttle the number of requests. "
                                + "This server thread is not in a bad state so starting a new one.", t);
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

    public void stop() throws Exception {
        log.info("stopping thrift server {}", getServerName());
        if (stopping.compareAndSet(false, true) && server != null) {
            log.debug("stopping serving");
            server.stop();
            log.debug("stopped serving");
        }
    }
}
