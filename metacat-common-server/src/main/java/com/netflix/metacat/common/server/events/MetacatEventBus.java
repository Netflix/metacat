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
import com.netflix.metacat.common.server.Config;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class MetacatEventBus {
    private final AsyncEventBus asyncEventBus;
    private final EventBus syncEventBus;

    @Inject
    public MetacatEventBus(Config config) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("metacat-event-pool-%d").build();
        int threadCount = config.getEventBusThreadCount();
        this.asyncEventBus = new AsyncEventBus(
                "metacat-async-event-bus",
                Executors.newFixedThreadPool(threadCount, threadFactory)
        );
        this.syncEventBus = new EventBus("metacat-sync-event-bus");
    }

    public void postAsync(Object event) {
        this.asyncEventBus.post(event);
    }

    public void postSync(Object event) {
        this.syncEventBus.post(event);
    }

    public void register(Object object) {
        asyncEventBus.register(object);
        syncEventBus.register(object);
    }

    public void unregister(Object object) {
        asyncEventBus.unregister(object);
        syncEventBus.unregister(object);
    }
}
