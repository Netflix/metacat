package com.netflix.metacat.common.server.monitoring;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.impl.AtomicDouble;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.inject.Singleton;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Class of dynamic Gauges.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
@Singleton
public final class DynamicGauges {
    private static final String DEFAULT_EXPIRATION = "15";
    private static final String DEFAULT_EXPIRATION_UNIT = "MINUTES";
    private static final String CLASS_NAME = DynamicGauges.class.getCanonicalName();
    private static final String EXPIRATION_PROP = CLASS_NAME + ".expiration";
    private static final String EXPIRATION_PROP_UNIT = CLASS_NAME + ".expirationUnit";

    private final LoadingCache<Id, AtomicDouble> gauges;

    /**
     * DynamicGauges class.
     */
    public DynamicGauges() {
        final String expiration = System.getProperty(EXPIRATION_PROP, DEFAULT_EXPIRATION);
        final String expirationUnit = System.getProperty(EXPIRATION_PROP_UNIT, DEFAULT_EXPIRATION_UNIT);
        final long expirationValue = Long.parseLong(expiration);
        final TimeUnit expirationUnitValue = TimeUnit.valueOf(expirationUnit);
        gauges = CacheBuilder.newBuilder()
                .expireAfterAccess(expirationValue, expirationUnitValue)
                .build(new CacheLoader<Id, AtomicDouble>() {
                    @Override
                    public AtomicDouble load(
                            @Nonnull final Id id) throws Exception {
                        return Spectator.globalRegistry().gauge(id, new AtomicDouble(0.0));
                    }
                });
    }

    /**
     * Set a gauge based on a given Id by a given value.
     *
     * @param id    The createdid
     * @param value The amount added to the current value
     */
    public void set(final Id id, final double value) {
        get(id).set(value);
    }

    /**
     * Increment a gauge specified by a name.
     *
     * @param name  name
     * @param value value
     */
    public void set(final String name, final double value) {
        set(Spectator.globalRegistry().createId(name), value);
    }

    private AtomicDouble get(final Id id) {
        try {
            return gauges.get(id);
        } catch (ExecutionException e) {
            log.error("Failed to get a gauge for {}: {}", id, e.getMessage());
            throw Throwables.propagate(e);
        }
    }
}
