package com.netflix.metacat.connector.polaris.routing;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

/**
 * Routing datasource to determine the correct type.
 */
public class RoutingDataSource extends AbstractRoutingDataSource {
    @Override
    protected Object determineCurrentLookupKey() {
        return DataSourceContextHolder.getContext();
    }
}
