package com.netflix.metacat.connector.hive.util;

/**
 * HiveConfigConstants.
 *
 * Updated for use with Hibernate.
 *
 * @since 1.0.0
 */
public final class HiveConfigConstants {
    /**
     * HIVE_METASTORE_TIMEOUT.
     */
    public static final String HIVE_METASTORE_TIMEOUT = "hive.metastore-timeout";

    /**
     * hive thrift port.
     */
    public static final String THRIFT_URI = "hive.metastore.uris";

    /**
     * USE_EMBEDDED_METASTORE.
     */
    public static final String USE_EMBEDDED_METASTORE = "hive.use.embedded.metastore";

    /**
     * ALLOW_RENAME_TABLE.
     */
    public static final String ALLOW_RENAME_TABLE = "hive.allow-rename-table";

    /**
     * USE_FASTPARTITION_SERVICE.
     */
    public static final String USE_FASTHIVE_SERVICE = "hive.use.embedded.fastservice";

    /**
     * ENABLE_AUDIT_PROCESSING.
     */
    public static final String ENABLE_AUDIT_PROCESSING = "hive.use.embedded.fastservice.auditEnabled";

    /**
     * GET_PARTITION_DETAILS_TIMEOUT.
     */
    public static final String GET_PARTITION_DETAILS_TIMEOUT = "hive.use.embedded.GetPartitionDetailsTimeout";

    /**
     * GET_ICEBERG_PARTITIONS_TIMEOUT.
     */
    public static final String GET_ICEBERG_PARTITIONS_TIMEOUT = "hive.iceberg.GetIcebergPartitionsTimeout";

    /**
     * USE_FAST_DELETION.
     */
    public static final String USE_FAST_DELETION = "hive.use.embedded.sql.delete.partitions";

    /**
     * THREAD_POOL_SIZE.
     */
    public static final String THREAD_POOL_SIZE = "hive.thread.pool.size";

    /**
     * USE_METASTORE_LOCAL.
     */
    public static final String USE_METASTORE_LOCAL = "hive.metastore.local";

    /**
     * JAVAX_JDO_OPTION_NAME.
     */
    public static final String JAVAX_JDO_OPTION_NAME = "javax.jdo.option.name";

    /**
     * Hibernate equivalent for datastore timeout.
     */
    public static final String HIBERNATE_JDBC_TIMEOUT = "hibernate.jdbc.timeout";

    /**
     * Hibernate equivalent for connection isolation level.
     */
    public static final String HIBERNATE_CONNECTION_ISOLATION = "hibernate.connection.isolation";

    /**
     * Hibernate caching configuration.
     */
    public static final String HIBERNATE_CACHE_USE_SECOND_LEVEL_CACHE = "hibernate.cache.use_second_level_cache";
    public static final String HIBERNATE_CACHE_REGION_FACTORY_CLASS = "hibernate.cache.region.factory_class";

    /**
     * Hibernate auto schema update.
     */
    public static final String HIBERNATE_HBM2DDL_AUTO = "hibernate.hbm2ddl.auto";

    /**
     * HIVE_METASTORE_DS_RETRY.
     */
    public static final String HIVE_METASTORE_DS_RETRY = "hive.metastore.ds.retry.attempts";

    /**
     * HIVE_HMSHANDLER_RETRY.
     */
    public static final String HIVE_HMSHANDLER_RETRY = "hive.hmshandler.retry.attempts";

    /**
     * HIVE_STATS_AUTOGATHER.
     */
    public static final String HIVE_STATS_AUTOGATHER = "hive.stats.autogather";

    /**
     * HIVE_HMSHANDLER_NAME.
     */
    public static final String HIVE_HMSHANDLER_NAME = "metacat";

    /**
     * METACAT_JDO_TIMEOUT.
     */
    public static final String METACAT_JDO_TIMEOUT = "metacat.jdo.timeout";

    /**
     * Configuration to convert a table to external on rename table.
     */
    public static final String ON_RENAME_CONVERT_TO_EXTERNAL = "metacat.on-rename-convert-to-external";

    private HiveConfigConstants() {
    }
}
