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

import com.google.common.collect.Maps;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.common.server.util.RegistryUtil;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Map;

/**
 * Event bus implementation using Springs Event Multicaster. By default, Spring supports synchronous event publishing.
 * This implementation supports both synchronous and asynchronous event publishing. If the listener is annotated with
 * AsyncListener, the event will be published asynchronously using a separate executor pool.
 * This implementation should be used as the application event multicaster in the Springs context.
 * Synchronous publishing of events is handled by this class and the asynchronous publishing of events is handled by
 * the asyncEventMulticaster.
 *
 * @author amajumdar
 * @since 1.1.x
 */
@Slf4j
public class MetacatApplicationEventMulticaster extends SimpleApplicationEventMulticaster {
    //Map of event multicasters keyed by the listener class name.
    private final Map<String, ApplicationEventMulticaster> asyncEventMulticasters = Maps.newHashMap();
    private final MeterRegistry registry;
    private final MetacatProperties metacatProperties;
    /**
     * Constructor.
     *
     * @param registry         registry for micrometer
     * @param metacatProperties The metacat properties to get number of executor threads from.
     *                          Likely best to do one more than number of CPUs
     */
    public MetacatApplicationEventMulticaster(final MeterRegistry registry, final MetacatProperties metacatProperties) {
        super();
        this.registry = registry;
        this.metacatProperties = metacatProperties;
    }

    /**
     * Post event. Events will be handles synchronously or asynchronously based on the listener annotation.
     *
     * @param event event
     */
    public void post(final ApplicationEvent event) {
        super.multicastEvent(event);
        asyncEventMulticasters.values().forEach(aem -> aem.multicastEvent(event));
    }

    @Override
    public void addApplicationListener(final ApplicationListener listener) {
        if (isAsyncListener(listener)) {
            final Class<?> clazz = getListenerTargetClass(listener);
            final String clazzName = clazz.getName();
            if (!asyncEventMulticasters.containsKey(clazzName)) {
                // Using simple name of the class to use for registering it with registry.
                // There is a chance of name collision if two class names are the same under different packages.
                asyncEventMulticasters.put(clazzName, createApplicationEventMultiCaster(clazz.getSimpleName()));
            }
            asyncEventMulticasters.get(clazzName).addApplicationListener(listener);
        } else {
            super.addApplicationListener(listener);
        }
    }

    private boolean isAsyncListener(final ApplicationListener listener) {
        return listener.getClass().isAnnotationPresent(AsyncListener.class)
            || (listener instanceof MetacatApplicationListenerMethodAdapter
                && ((MetacatApplicationListenerMethodAdapter) listener).getTargetClass()
                .isAnnotationPresent(AsyncListener.class));
    }

    private Class<?> getListenerTargetClass(final ApplicationListener listener) {
        return listener instanceof MetacatApplicationListenerMethodAdapter
            ? ((MetacatApplicationListenerMethodAdapter) listener).getTargetClass() : listener.getClass();
    }

    private ApplicationEventMulticaster createApplicationEventMultiCaster(final String name) {
        final SimpleApplicationEventMulticaster result = new SimpleApplicationEventMulticaster();
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(metacatProperties.getEvent().getBus().getExecutor().getThread().getCount());
        executor.initialize();
        RegistryUtil.registerThreadPool(registry, "metacat.event.pool." + name,
            executor.getThreadPoolExecutor());
        result.setTaskExecutor(executor);
        return result;
    }

    @Override
    public void removeApplicationListener(final ApplicationListener listener) {
        super.removeApplicationListener(listener);
        asyncEventMulticasters.values().forEach(aem -> aem.removeApplicationListener(listener));
    }

    @Override
    public void removeAllListeners() {
        super.removeAllListeners();
        asyncEventMulticasters.values().forEach(ApplicationEventMulticaster::removeAllListeners);
    }
}
