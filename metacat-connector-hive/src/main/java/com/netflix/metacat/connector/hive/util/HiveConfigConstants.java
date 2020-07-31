/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive.util;

/**
 * HiveConfigConstants.
 *
 * @author zhenl
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
     * USE_FASTPARTITION_SERVICE.
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
     * JAVAX_JDO_DATASTORETIMEOUT.
     */
    public static final String JAVAX_JDO_DATASTORETIMEOUT = "javax.jdo.option.DatastoreTimeout";
    /**
     * JAVAX_JDO_DATASTOREREADTIMEOUT.
     */
    public static final String JAVAX_JDO_DATASTOREREADTIMEOUT = "javax.jdo.option.DatastoreReadTimeoutMillis";
    /**
     * JAVAX_JDO_DATASTOREWRITETIMEOUT.
     */
    public static final String JAVAX_JDO_DATASTOREWRITETIMEOUT = "javax.jdo.option.DatastoreWriteTimeoutMillis";
    /**
     * JAVAX_JDO_PERSISTENCEMANAGER_FACTORY_CLASS.
     */
    public static final String JAVAX_JDO_PERSISTENCEMANAGER_FACTORY_CLASS = "javax.jdo.PersistenceManagerFactoryClass";
    /**
     * JAVAX_JDO_PERSISTENCEMANAGER_FACTORY.
     */
    public static final String JAVAX_JDO_PERSISTENCEMANAGER_FACTORY
            = "com.netflix.metacat.connector.hive.client.embedded.HivePersistenceManagerFactory";
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
     * DATANUCLEUS_AUTOSTARTMECHANISMMODE.
     */
    public static final String DATANUCLEUS_AUTOSTARTMECHANISMMODE = "datanucleus.autoStartMechanismMode";
    /**
     * DATANUCLEUS_DETACHALLONCOMMIT.
     */
    public static final String DATANUCLEUS_DETACHALLONCOMMIT = "datanucleus.detachAllOnCommit";
    /**
     * DATANUCLEUS_DETACHALLONROLLBACK.
     */
    public static final String DATANUCLEUS_DETACHALLONROLLBACK = "datanucleus.detachAllOnRollback";

    /**
     * DATANUCLEUS_PERSISTENCYBYREACHATCOMMIT.
     */
    public static final String DATANUCLEUS_PERSISTENCYBYREACHATCOMMIT = "datanucleus.persistenceByReachabilityAtCommit";

    /**
     * DATANUCLEUS_CACHE_LEVEL2_TYPE.
     */
    public static final String DATANUCLEUS_CACHE_LEVEL2_TYPE = "datanucleus.cache.level2.type";
    /**
     * DATANUCLEUS_CACHE_LEVEL2.
     */
    public static final String DATANUCLEUS_CACHE_LEVEL2 = "datanucleus.cache.level2";
    /**
     * DATANUCLEUS_VALIDATECOLUMNS.
     */
    public static final String DATANUCLEUS_VALIDATECOLUMNS = "datanucleus.validateColumns";
    /**
     * DATANUCLEUS_VALIDATECONSTRAINTS.
     */
    public static final String DATANUCLEUS_VALIDATECONSTRAINTS = "datanucleus.validateConstraints";
    /**
     * DATANUCLEUS_VALIDATETABLE.
     */
    public static final String DATANUCLEUS_VALIDATETABLE = "datanucleus.validateTables";
    /**
     * DATANUCLEUS_TRANSACTIONISOLATION.
     */
    public static final String DATANUCLEUS_TRANSACTIONISOLATION = "datanucleus.transactionIsolation";
    /**
     * DATANUCLEUS_READCOMMITTED.
     */
    public static final String DATANUCLEUS_READCOMMITTED = "read-committed";
    /**
     * DATANUCLEUS_FIXEDDATASTORE.
     */
    public static final String DATANUCLEUS_FIXEDDATASTORE = "datanucleus.fixedDatastore";

    /**
     * DATANUCLEUS_AUTOCREATESCHEMA.
     */
    public static final String DATANUCLEUS_AUTOCREATESCHEMA = "datanucleus.autoCreateSchema";
    /**
     * DATANUCLEUS_RDBMS_CHECKEXISTTABLESORVIEWS.
     */
    public static final String DATANUCLEUS_RDBMS_CHECKEXISTTABLESORVIEWS = "datanucleus.rdbms.CheckExistTablesOrViews";
    /**
     * DATANUCLEUS_RDBMS_INITIALIZECOULUMNINFO.
     */
    public static final String DATANUCLEUS_RDBMS_INITIALIZECOULUMNINFO = "datanucleus.rdbms.initializeColumnInfo";
    /**
     * DATANUCLEUS_IDENTIFIERFACTORY.
     */
    public static final String DATANUCLEUS_IDENTIFIERFACTORY = "datanucleus.identifierFactory";

    /**
     * DATANUCLEUS_DATANUCLEU1.
     */
    public static final String DATANUCLEUS_DATANUCLEU1 = "datanucleus1";

    /**
     * DATANUCLEUS_CONNECTIONFACTORY.
     */
    public static final String DATANUCLEUS_CONNECTIONFACTORY = "datanucleus.ConnectionFactory";

    /**
     * DATANUCLEUS_RDBMS_USELEGACYNATIVEVALUESTRATEGY.
     */
    public static final String DATANUCLEUS_RDBMS_USELEGACYNATIVEVALUESTRATEGY
            = "datanucleus.rdbms.useLegacyNativeValueStrategy";

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
