/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.connector.hive.configs;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService;
import com.netflix.metacat.connector.hive.HiveConnectorPartitionService;
import com.netflix.metacat.connector.hive.HiveConnectorTableService;
import com.netflix.metacat.common.server.connectors.util.TimeUtil;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.client.thrift.HiveMetastoreClientFactory;
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Hive configs.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Slf4j
@Configuration
public class HiveConnectorConfig {
    /** Default Query timeout in milliseconds. */
    private static final int DEFAULT_DATASTORE_TIMEOUT = 60000;
    /** Default Query timeout in milliseconds for reads. */
    private static final int DEFAULT_DATASTORE_READ_TIMEOUT = 120000;
    /** Default Query timeout in milliseconds for writes. */
    private static final int DEFAULT_DATASTORE_WRITE_TIMEOUT = 120000;

    /**
     * create hive connector database service.
     *
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @return HiveConnectorDatabaseService
     */
    @Bean
    @ConditionalOnMissingBean(HiveConnectorDatabaseService.class)
    public HiveConnectorDatabaseService hiveDatabaseService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter
    ) {
        return new HiveConnectorDatabaseService(
            metacatHiveClient,
            hiveMetacatConverter
        );
    }

    /**
     * create hive connector table service.
     *
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param connectorContext             connector config
     * @return HiveConnectorTableService
     */
    @Bean
    @ConditionalOnMissingBean(HiveConnectorTableService.class)
    public HiveConnectorTableService hiveTableService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final ConnectorContext connectorContext
    ) {
        return new HiveConnectorTableService(
            connectorContext.getCatalogName(),
            metacatHiveClient,
            hiveConnectorDatabaseService,
            hiveMetacatConverters,
            connectorContext
        );
    }

    /**
     * create hive connector partition service.
     *
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @param connectorContext     connector config
     * @return HiveConnectorPartitionService
     */
    @Bean
    @ConditionalOnMissingBean(HiveConnectorPartitionService.class)
    public HiveConnectorPartitionService partitionService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter,
        final ConnectorContext connectorContext
    ) {
        return new HiveConnectorPartitionService(
            connectorContext,
            metacatHiveClient,
            hiveMetacatConverter
        );
    }

    /**
     * create thrift hive client.
     *
     * @param connectorContext connector config.
     * @return data source
     * @throws MetaException meta exception
     */
    @Bean
    @ConditionalOnMissingBean(IMetacatHiveClient.class)
    public IMetacatHiveClient createThriftClient(final ConnectorContext connectorContext) throws MetaException {
        final HiveMetastoreClientFactory factory = new HiveMetastoreClientFactory(
            null,
            (int) TimeUtil.toTime(
                connectorContext.getConfiguration().getOrDefault(HiveConfigConstants.HIVE_METASTORE_TIMEOUT, "20s"),
                TimeUnit.SECONDS,
                TimeUnit.MILLISECONDS
            )
        );
        final String metastoreUri = connectorContext.getConfiguration().get(HiveConfigConstants.THRIFT_URI);
        try {
            return new MetacatHiveClient(
                connectorContext.getCatalogName(),
                new URI(metastoreUri),
                factory,
                connectorContext.getRegistry()
            );
        } catch (Exception e) {
            final String message = String.format("Invalid thrift uri %s", metastoreUri);
            log.info(message);
            throw new IllegalArgumentException(message, e);
        }
    }

    /**
     * thread Service Manager.
     * @param connectorContext connector config
     * @return threadServiceManager
     */
    @Bean
    public ThreadServiceManager threadServiceManager(final ConnectorContext connectorContext) {
        return new ThreadServiceManager(connectorContext.getRegistry(),
            connectorContext.getConfig().getServiceMaxNumberOfThreads(),
            1000,
            "hive");
    }

    /**
     * create warehouse for file system calls.
     *
     * @param connectorContext connector config context
     * @return WareHouse
     */
    @Bean
    public Warehouse warehouse(final ConnectorContext connectorContext) {
        try {
            final HiveConf conf = this.getDefaultConf(connectorContext);
            connectorContext.getConfiguration().forEach(conf::set);
            return new Warehouse(conf);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format(
                    "Failed creating the hive warehouse for catalog: %s",
                    connectorContext.getCatalogName()
                ),
                e
            );
        }
    }

    /**
     * hive DataSource.
     *
     * @param connectorContext connector config.
     * @return data source
     */
    @Bean
    public DataSource hiveDataSource(final ConnectorContext connectorContext) {
        final HiveConf conf = this.getDefaultConf(connectorContext);
        connectorContext.getConfiguration().forEach(conf::set);
        DataSourceManager.get().load(
            connectorContext.getCatalogShardName(),
            connectorContext.getConfiguration()
        );
        return DataSourceManager.get().get(connectorContext.getCatalogShardName());
    }

    /**
     * hive metadata Transaction Manager.
     *
     * @param hiveDataSource hive data source
     * @return hive transaction manager
     */
    @Bean
    public DataSourceTransactionManager hiveTxManager(
        @Qualifier("hiveDataSource") final DataSource hiveDataSource) {
        return new DataSourceTransactionManager(hiveDataSource);
    }

    /**
     * hive metadata read JDBC template. Query timeout is set to control long running read queries.
     *
     * @param connectorContext connector config.
     * @param hiveDataSource hive data source
     * @return hive JDBC Template
     */
    @Bean
    public JdbcTemplate hiveReadJdbcTemplate(
        final ConnectorContext connectorContext,
        @Qualifier("hiveDataSource") final DataSource hiveDataSource) {
        final JdbcTemplate result = new JdbcTemplate(hiveDataSource);
        result.setQueryTimeout(getDataStoreReadTimeout(connectorContext) / 1000);
        return result;
    }

    /**
     * hive metadata write JDBC template. Query timeout is set to control long running write queries.
     *
     * @param connectorContext connector config.
     * @param hiveDataSource hive data source
     * @return hive JDBC Template
     */
    @Bean
    public JdbcTemplate hiveWriteJdbcTemplate(
        final ConnectorContext connectorContext,
        @Qualifier("hiveDataSource") final DataSource hiveDataSource) {
        final JdbcTemplate result = new JdbcTemplate(hiveDataSource);
        result.setQueryTimeout(getDataStoreWriteTimeout(connectorContext) / 1000);
        return result;
    }

    @VisibleForTesting
    private HiveConf getDefaultConf(
        final ConnectorContext connectorContext
    ) {
        final HiveConf result = new HiveConf();
        result.setBoolean(HiveConfigConstants.USE_METASTORE_LOCAL, true);

        final int dataStoreTimeout = getDataStoreTimeout(connectorContext);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTORETIMEOUT, dataStoreTimeout);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTOREREADTIMEOUT, dataStoreTimeout);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTOREWRITETIMEOUT, getDataStoreWriteTimeout(connectorContext));
        result.setInt(HiveConfigConstants.HIVE_METASTORE_DS_RETRY, 0);
        result.setInt(HiveConfigConstants.HIVE_HMSHANDLER_RETRY, 0);
        result.set(
            HiveConfigConstants.JAVAX_JDO_PERSISTENCEMANAGER_FACTORY_CLASS,
            HiveConfigConstants.JAVAX_JDO_PERSISTENCEMANAGER_FACTORY
        );
        result.setBoolean(HiveConfigConstants.HIVE_STATS_AUTOGATHER, false);
        return result;
    }

    private int getDataStoreTimeout(final ConnectorContext connectorContext) {
        int result = DEFAULT_DATASTORE_TIMEOUT;
        try {
            result = Integer.parseInt(
                connectorContext.getConfiguration().get(HiveConfigConstants.JAVAX_JDO_DATASTORETIMEOUT));
        } catch (final Exception ignored) { }
        return result;
    }

    private int getDataStoreReadTimeout(final ConnectorContext connectorContext) {
        int result = DEFAULT_DATASTORE_READ_TIMEOUT;
        try {
            result = Integer.parseInt(
                connectorContext.getConfiguration().get(HiveConfigConstants.JAVAX_JDO_DATASTOREREADTIMEOUT));
        } catch (final Exception ignored) { }
        return result;
    }

    private int getDataStoreWriteTimeout(final ConnectorContext connectorContext) {
        int result = DEFAULT_DATASTORE_WRITE_TIMEOUT;
        try {
            result = Integer.parseInt(
                connectorContext.getConfiguration().get(HiveConfigConstants.JAVAX_JDO_DATASTOREWRITETIMEOUT));
        } catch (final Exception ignored) { }
        return result;
    }
}
