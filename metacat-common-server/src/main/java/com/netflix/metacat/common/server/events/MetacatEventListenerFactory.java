/*
 *  Copyright 2018 Netflix, Inc.
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
 */

package com.netflix.metacat.common.server.events;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.EventListenerFactory;

import java.lang.reflect.Method;

/**
 * This class overrides the DefaultEventListenerFactory in the Spring container.
 *
 * @author amajumdar
 * @since 1.2.x
 */
public class MetacatEventListenerFactory implements EventListenerFactory {
    @Override
    public boolean supportsMethod(final Method method) {
        return true;
    }

    @Override
    public ApplicationListener<?> createApplicationListener(final String beanName,
                                                            final Class<?> type,
                                                            final Method method) {
        return new MetacatApplicationListenerMethodAdapter(beanName, type, method);
    }
}
