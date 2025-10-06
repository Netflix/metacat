package com.netflix.metacat.connector.polaris.store;

import org.springframework.data.domain.Pageable;

import java.util.stream.Collectors;

/**
 * Utility class for generating SQL clauses.
 */
public final class Utils {

    // Private constructor to prevent instantiation
    private Utils() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Generates an SQL ORDER BY clause based on the Pageable object.
     *
     * @param page the Pageable object containing sort information
     * @return the ORDER BY clause as a String
     */
    public static String generateOrderBy(final Pageable page) {
        String orderBy = "";
        if (page.getSort().isSorted()) {
            orderBy = page.getSort().stream()
                .map(order -> order.getProperty() + " " + order.getDirection())
                .collect(Collectors.joining(", "));
            orderBy = " ORDER BY " + orderBy;
        }
        return orderBy;
    }
}
