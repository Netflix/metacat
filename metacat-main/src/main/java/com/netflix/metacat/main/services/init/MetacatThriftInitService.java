package com.netflix.metacat.main.services.init;

import com.google.common.base.Throwables;
import com.netflix.metacat.main.services.MetacatThriftService;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Inits the thrift service.
 */
@Slf4j
@Getter
@Accessors(fluent = true)
@RequiredArgsConstructor
public class MetacatThriftInitService {
    @NonNull
    private final MetacatThriftService metacatThriftService;
    @NonNull
    private final MetacatCoreInitService coreInitService;

    // Initial values are false
    private final AtomicBoolean thriftStarted = new AtomicBoolean();

    /**
     * Metacat service shutdown.
     */
    public void stop() {
        log.info("Metacat application is stopped. Stopping services.");
        try {
            this.metacatThriftService.stop();
            this.thriftStarted.set(false);
            this.coreInitService.stop();
        } catch (final Exception e) {
            // Just log it since we're shutting down anyway shouldn't matter to propagate it
            log.error("Unable to properly shutdown services due to {}", e.getMessage(), e);
        }
        log.info("Finished stopping services.");
    }

    /**
     * Metacat service initialization.
     */
    public void start() {
        log.info("Metacat application starting. Starting internal services...");
        try {
            // TODO: Rather than doing this statically why don't we have things that need to be started implement
            //       some interface/order?
            this.coreInitService.start();
            this.metacatThriftService.start();
            this.thriftStarted.set(true);
        } catch (final Exception e) {
            log.error("Unable to initialize services due to {}", e.getMessage(), e);
            Throwables.propagate(e);
        }
        log.info("Finished starting internal services.");
    }
}
