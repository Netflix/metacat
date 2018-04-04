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

import org.springframework.context.event.ApplicationListenerMethodAdapter;

import java.lang.reflect.Method;

/**
 * This class has been introduced to get access to the targetClass in ApplicationListenerMethodAdapter.
 *
 * @author amajumdar
 * @since 1.2.x
 */
public class MetacatApplicationListenerMethodAdapter extends ApplicationListenerMethodAdapter {
    private final Class<?> targetClass;

    /**
     * Constructor.
     * @param beanName bean name
     * @param targetClass bean class
     * @param method bean method
     */
    public MetacatApplicationListenerMethodAdapter(final String beanName,
                                                   final Class<?> targetClass,
                                                   final Method method) {
        super(beanName, targetClass, method);
        this.targetClass = targetClass;
    }

    public Class<?> getTargetClass() {
        return targetClass;
    }
}
