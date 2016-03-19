package com.netflix.metacat.common.monitoring;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.CompositeMonitor;
import com.netflix.servo.monitor.DoubleGauge;
import com.netflix.servo.monitor.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.tag.TagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Utility class that dynamically creates gauges based on an arbitrary (name, tagList), or {@link com.netflix.servo.monitor.MonitorConfig}
 * Gauges are automatically expired after 15 minutes of inactivity.
 */
public final class DynamicGauge implements CompositeMonitor<Long> {
    private static final Logger log = LoggerFactory.getLogger(DynamicGauge.class);
    private static final String DEFAULT_EXPIRATION = "15";
    private static final String DEFAULT_EXPIRATION_UNIT = "MINUTES";
    private static final String CLASS_NAME = DynamicGauge.class.getCanonicalName();
    private static final String EXPIRATION_PROP = CLASS_NAME + ".expiration";
    private static final String EXPIRATION_PROP_UNIT = CLASS_NAME + ".expirationUnit";
    private static final String INTERNAL_ID = "servoGauges";
    private static final String CACHE_MONITOR_ID = "servoGaugesCache";
    private static final MonitorConfig BASE_CONFIG = new MonitorConfig.Builder(INTERNAL_ID).build();

    private static final DynamicGauge INSTANCE = new DynamicGauge();

    private final LoadingCache<MonitorConfig, DoubleGauge> gauges;
    private final CompositeMonitor<?> cacheMonitor;

    private DynamicGauge() {
        final String expiration = System.getProperty(EXPIRATION_PROP, DEFAULT_EXPIRATION);
        final String expirationUnit = System.getProperty(EXPIRATION_PROP_UNIT, DEFAULT_EXPIRATION_UNIT);
        final long expirationValue = Long.parseLong(expiration);
        final TimeUnit expirationUnitValue = TimeUnit.valueOf(expirationUnit);

        gauges = CacheBuilder.newBuilder()
                .expireAfterAccess(expirationValue, expirationUnitValue)
                .build(new CacheLoader<MonitorConfig, DoubleGauge>() {
                    @Override
                    public DoubleGauge load(@Nonnull final MonitorConfig config) throws Exception {
                        return new DoubleGauge(config);
                    }
                });
        cacheMonitor = Monitors.newCacheMonitor(CACHE_MONITOR_ID, gauges);
        DefaultMonitorRegistry.getInstance().register(this);
    }

    /**
     * Set a gauge based on a given {@link MonitorConfig} by a given value.
     *
     * @param config The monitoring config
     * @param value  The amount added to the current value
     */
    public static void set(MonitorConfig config, double value) {
        INSTANCE.get(config).set(value);
    }

    /**
     * Increment a gauge specified by a name.
     */
    public static void set(String name, double value) {
        set(MonitorConfig.builder(name).build(), value);
    }

    /**
     * Set the gauge for a given name, tagList by a given value.
     */
    public static void set(String name, TagList list, double value) {
        final MonitorConfig config = MonitorConfig.builder(name).withTags(list).build();
        set(config, value);
    }

    private DoubleGauge get(MonitorConfig config) {
        try {
            return gauges.get(config);
        } catch (ExecutionException e) {
            log.error("Failed to get a gauge for {}: {}", config, e.getMessage());
            throw Throwables.propagate(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Monitor<?>> getMonitors() {
        final ConcurrentMap<MonitorConfig, DoubleGauge> gaugesMap = gauges.asMap();
        return ImmutableList.copyOf(gaugesMap.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getValue() {
        return (long) gauges.asMap().size();
    }

    @Override
    public Long getValue(int pollerIndex) {
        return getValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MonitorConfig getConfig() {
        return BASE_CONFIG;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ConcurrentMap<?, ?> map = gauges.asMap();
        return MoreObjects.toStringHelper(this)
                .add("baseConfig", BASE_CONFIG)
                .add("totalGauges", map.size())
                .add("gauges", map)
                .toString();
    }
}

