package com.netflix.metacat.connector.polaris.routing;

/**
 * DataSourceEnum.
 */
public enum DataSourceEnum {
    /**
     * The primary (write) datasource.
     */
    PRIMARY,

    /**
     * The replica (read) datasource.
     */
    REPLICA
}
