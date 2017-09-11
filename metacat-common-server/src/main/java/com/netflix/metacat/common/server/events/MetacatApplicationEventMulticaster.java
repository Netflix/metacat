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

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.SimpleApplicationEventMulticaster;

import javax.annotation.Nonnull;

/**
 * Event bus implementation using Springs Event Multicaster. By default, Spring supports sunchronous event publishing.
 * This implementation supports both synchronous and asynchronous event publishing. At present, client explicitly
 * call postAsync or postSync to appropriately publish events.
 * This implementation should be used as the application event multicaster in the Springs context.
 * Synchronous publishing of events is handled by this class and the asynchronous publishing of events is handled by
 * the asyncEventMulticaster.
 *
 * @author amajumdar
 * @since 1.1.x
 */
@Slf4j
public class MetacatApplicationEventMulticaster extends SimpleApplicationEventMulticaster {

    private final ApplicationEventMulticaster asyncEventMulticaster;
    /**
     * Constructor.
     *
     * @param asyncEventMulticaster The asynchronous event multicaster to use
     */
    public MetacatApplicationEventMulticaster(
        @Nonnull @NonNull final ApplicationEventMulticaster asyncEventMulticaster
    ) {
        super();
        this.asyncEventMulticaster = asyncEventMulticaster;
    }

    /**
     * Post event asynchronously.
     *
     * @param event event
     */
    public void postAsync(final ApplicationEvent event) {
        this.asyncEventMulticaster.multicastEvent(event);
    }

    /**
     * Post event synchronously.
     *
     * @param event event
     */
    public void postSync(final ApplicationEvent event) {
        super.multicastEvent(event);
    }

    @Override
    public void addApplicationListener(final ApplicationListener listener) {
        super.addApplicationListener(listener);
        asyncEventMulticaster.addApplicationListener(listener);
    }

    @Override
    public void removeApplicationListener(final ApplicationListener listener) {
        super.removeApplicationListener(listener);
        asyncEventMulticaster.removeApplicationListener(listener);
    }

    @Override
    public void removeAllListeners() {
        super.removeAllListeners();
        asyncEventMulticaster.removeAllListeners();
    }
}
