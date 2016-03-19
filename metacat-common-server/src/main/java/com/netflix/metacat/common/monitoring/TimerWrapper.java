package com.netflix.metacat.common.monitoring;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Servo Timer wrapper
 *
 * @author amajumdar
 */
public class TimerWrapper {
    private static final Stopwatch NULL_STOPWATCH = new Stopwatch() {
        @Override
        public long getDuration(TimeUnit timeUnit) {
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
        public Long getValue(int pollerIndex) {
            return null;
        }

        @Override
        public Long getValue() {
            return null;
        }

        @Override
        public void record(long duration, TimeUnit timeUnit) {

        }

        @Override
        public void record(long duration) {

        }

        @Override
        public Stopwatch start() {
            return NULL_STOPWATCH;
        }
    };
    private static final LoadingCache<String, Timer> TIMERS = CacheBuilder.newBuilder()
            .build(
                    new CacheLoader<String, Timer>() {
                        public Timer load(@Nonnull String timerName) {
                            Timer timer = Monitors.newTimer(timerName);
                            DefaultMonitorRegistry.getInstance().register(timer);
                            return timer;
                        }
                    });
    private static final Logger log = LoggerFactory.getLogger(TimerWrapper.class);
    private final String name;
    private final Timer timer;
    private Stopwatch stopwatch;

    private TimerWrapper(String name) {
        this.name = name;
        Timer t = NULL_TIMER;
        try {
            t = TIMERS.get(name);
        } catch (ExecutionException ex) {
            log.warn("Error fetching timer: {}", name, ex);
        }
        this.timer = t;
    }

    public static TimerWrapper createStarted(String name) {
        TimerWrapper wrapper = new TimerWrapper(name);
        wrapper.start();
        return wrapper;
    }

    public static TimerWrapper createStopped(String name) {
        return new TimerWrapper(name);
    }

    public void start() {
        stopwatch = timer.start();
    }

    public long stop() {
        stopwatch.stop();
        return stopwatch.getDuration(TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return "Timer{" + name + " - " + stopwatch.getDuration(TimeUnit.MILLISECONDS) + "ms}";
    }
}
