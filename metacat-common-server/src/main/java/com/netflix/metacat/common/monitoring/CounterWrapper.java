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

package com.netflix.metacat.common.monitoring;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Monitors;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;

/**
 * Servo counter wrapper.
 *
 * @author amajumdar
 */
@Slf4j
public final class CounterWrapper {

    private static final LoadingCache<String, Counter> COUNTERS = CacheBuilder.newBuilder()
        .build(
            new CacheLoader<String, Counter>() {
                public Counter load(
                    @Nonnull
                    final String counterName) {
                    final Counter counter = Monitors.newCounter(counterName);
                    DefaultMonitorRegistry.getInstance().register(counter);
                    return counter;
                }
            });

    private CounterWrapper() {
    }

    /**
     * Increments the servo counter.
     * @param counterName counter name
     * @param incrementAmount increment value
     */
    public static void incrementCounter(final String counterName, final long incrementAmount) {
        try {
            final Counter counter = COUNTERS.get(counterName);
            if (incrementAmount == 1) {
                counter.increment();
            } else {
                counter.increment(incrementAmount);
            }
        } catch (ExecutionException ex) {
            log.warn("Error fetching counter: {}", counterName, ex);
        }
    }

    /**
     * Increments the servo counter by 1.
     * @param counterName counter name
     */
    public static void incrementCounter(final String counterName) {
        incrementCounter(counterName, 1);
    }
}
