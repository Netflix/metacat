package com.netflix.metacat.common.monitoring;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Monitors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;

/**
 * Servo counter wrapper
 *
 * @author amajumdar
 */
public class CounterWrapper {
    private static final LoadingCache<String, Counter> COUNTERS = CacheBuilder.newBuilder()
            .build(
                    new CacheLoader<String, Counter>() {
                        public Counter load(@Nonnull String counterName) {
                            Counter counter = Monitors.newCounter(counterName);
                            DefaultMonitorRegistry.getInstance().register(counter);
                            return counter;
                        }
                    });
    private static final Logger log = LoggerFactory.getLogger(CounterWrapper.class);

    public static void incrementCounter(String counterName, long incrementAmount) {
        try {
            Counter counter = COUNTERS.get(counterName);
            if (incrementAmount == 1) {
                counter.increment();
            } else {
                counter.increment(incrementAmount);
            }
        } catch (ExecutionException ex) {
            log.warn("Error fetching counter: {}", counterName, ex);
        }
    }

    public static void incrementCounter(String counterName) {
        incrementCounter(counterName, 1);
    }
}
