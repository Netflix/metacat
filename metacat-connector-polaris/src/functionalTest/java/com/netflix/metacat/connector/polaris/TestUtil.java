package com.netflix.metacat.connector.polaris;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class containing methods to aid in testing by simulating various conditions.
 */
@Slf4j
public final class TestUtil {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private TestUtil() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * Simulates a delay for a fixed period of time.
     */
    public static void simulateDelay() {
        try {
            Thread.sleep(5000); // 5 seconds delay
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
            log.debug("Sleep was interrupted", e);
        }
    }
}
