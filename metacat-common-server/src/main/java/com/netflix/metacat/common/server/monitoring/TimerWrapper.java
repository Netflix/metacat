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
package com.netflix.metacat.common.server.monitoring;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Servo Timer wrapper.
 *
 * @author amajumdar
 */
@Slf4j
public final class TimerWrapper {
    private static final Stopwatch NULL_STOPWATCH = new Stopwatch() {
        @Override
        public long getDuration(final TimeUnit timeUnit) {
            return 0;
        }

        @Override
        public long getDuration() {
            return 0;
        }

        @Override
        public void reset() {
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }
    };
    private static final Timer NULL_TIMER = new Timer() {
        @Override
        public MonitorConfig getConfig() {
            return null;
        }

        @Override
        public TimeUnit getTimeUnit() {
            return null;
        }

        @Override
        public Long getValue(final int pollerIndex) {
            return null;
        }

        @Override
        public Long getValue() {
            return null;
        }

        @Override
        public void record(final long duration, final TimeUnit timeUnit) {

        }

        @Override
        public void record(final long duration) {

        }

        @Override
        public Stopwatch start() {
            return NULL_STOPWATCH;
        }
    };
    private static final LoadingCache<String, Timer> TIMERS = CacheBuilder.newBuilder()
        .build(
            new CacheLoader<String, Timer>() {
                public Timer load(
                    @Nonnull final String timerName) {
                    final Timer timer = Monitors.newTimer(timerName);
                    DefaultMonitorRegistry.getInstance().register(timer);
                    return timer;
                }
            });
    private final String name;
    private final Timer timer;
    private Stopwatch stopwatch;

    private TimerWrapper(final String name) {
        this.name = name;
        Timer t = NULL_TIMER;
        try {
            t = TIMERS.get(name);
        } catch (ExecutionException ex) {
            log.warn("Error fetching timer: {}", name, ex);
        }
        this.timer = t;
    }

    /**
     * Creates the timer.
     *
     * @param name name of the timer
     * @return TimerWrapper
     */
    public static TimerWrapper createStarted(final String name) {
        final TimerWrapper wrapper = new TimerWrapper(name);
        wrapper.start();
        return wrapper;
    }

    /**
     * Creates the timer.
     *
     * @param name name of the timer
     * @return TimerWrapper
     */
    public static TimerWrapper createStopped(final String name) {
        return new TimerWrapper(name);
    }

    /**
     * Starts the timer.
     */
    public void start() {
        stopwatch = timer.start();
    }

    /**
     * Stops the timer.
     *
     * @return duration in milliseconds
     */
    public long stop() {
        stopwatch.stop();
        return stopwatch.getDuration(TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return "Timer{" + name + " - " + stopwatch.getDuration(TimeUnit.MILLISECONDS) + "ms}";
    }
}
