package com.netflix.metacat.connector.polaris.store.base;

import lombok.Getter;
import org.springframework.data.domain.Pageable;

import java.util.stream.Collectors;

/**
 * BasePolarisCustomRepository.
 */
@Getter
public class BasePolarisReplicaRepository {
    protected String generateOrderBy(final Pageable page) {
        // Generate ORDER BY clause
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
