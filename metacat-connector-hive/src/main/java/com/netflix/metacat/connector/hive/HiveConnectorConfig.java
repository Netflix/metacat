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

package com.netflix.metacat.connector.hive;

import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.common.server.util.MetacatConnectorConfig;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.client.embedded.EmbeddedHiveClient;
import com.netflix.metacat.connector.hive.client.thrift.HiveMetastoreClientFactory;
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.metastore.HMSHandlerProxy;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

    /**
     * create hive connector partition service.
     *
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @param connectorConfig      connector config
     * @return HiveConnectorPartitionService
     */
    @Bean
    @ConditionalOnProperty(value = "useHiveFastService", havingValue = "false")
    public HiveConnectorPartitionService partitionService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter,
        final MetacatConnectorConfig connectorConfig
    ) {
        return new HiveConnectorPartitionService(
            connectorConfig.getCatalogName(), metacatHiveClient, hiveMetacatConverter);
    }

    /**
     * create hive connector fast partition service.
     *
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @param threadServiceManager thread service manager
     * @param connectorConfig      connector config
     * @param dataSource           data source
     * @return HiveConnectorPartitionService
     */
    @Bean
    @ConditionalOnProperty(value = "useHiveFastService", havingValue = "true")
    public HiveConnectorFastPartitionService fastHivePartitionService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter,
        final ThreadServiceManager threadServiceManager,
        final MetacatConnectorConfig connectorConfig,
        @Qualifier("hiveDataSource") final DataSource dataSource
    ) {
        return new HiveConnectorFastPartitionService(connectorConfig.getCatalogName(),
            metacatHiveClient, hiveMetacatConverter,
            connectorConfig, threadServiceManager,
            dataSource);
    }


    /**
     * create hive connector database service.
     *
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @param connectorConfig      connector config
     * @return HiveConnectorDatabaseService
     */
    @Bean
    public HiveConnectorDatabaseService hiveDatabaseService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter,
        final MetacatConnectorConfig connectorConfig) {
        return new HiveConnectorDatabaseService(connectorConfig.getCatalogName(),
            metacatHiveClient, hiveMetacatConverter);
    }

    /**
     * create hive connector table service.
     *
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param metacatConnectorConfig       connector config
     * @return HiveConnectorTableService
     */
    @Bean
    @ConditionalOnProperty(value = "useHiveFastService", havingValue = "false")
    public HiveConnectorTableService hiveTableService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final MetacatConnectorConfig metacatConnectorConfig) {
        return new HiveConnectorTableService(metacatConnectorConfig.getCatalogName(), metacatHiveClient,
            hiveConnectorDatabaseService, hiveMetacatConverters, Boolean.parseBoolean(
            metacatConnectorConfig.getConfiguration()
                .getOrDefault(HiveConfigConstants.ALLOW_RENAME_TABLE, "false")));
    }

    /**
     * create hive connector fast table service.
     *
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param metacatConnectorConfig       server context
     * @param dataSource                   data source
     * @return HiveConnectorFastTableService
     */
    @Bean
    @ConditionalOnProperty(value = "useHiveFastService", havingValue = "true")
    public HiveConnectorFastTableService fastHiveTableService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final MetacatConnectorConfig metacatConnectorConfig,
        @Qualifier("hiveDataSource") final DataSource dataSource

    ) {
        return new HiveConnectorFastTableService(
            metacatConnectorConfig.getCatalogName(), metacatHiveClient,
            hiveConnectorDatabaseService, hiveMetacatConverters,
            Boolean.parseBoolean(
                metacatConnectorConfig.getConfiguration()
                    .getOrDefault(HiveConfigConstants.ALLOW_RENAME_TABLE, "false")),
            metacatConnectorConfig,
            dataSource);
    }

    /**
     * create local hive client.
     *
     * @param metacatConnectorConfig connector config context
     * @return IMetacatHiveClient
     * @throws Exception exception
     */

    @Bean
    @ConditionalOnProperty(value = "useLocalMetastore", havingValue = "true")
    public IMetacatHiveClient createLocalClient(
        final MetacatConnectorConfig metacatConnectorConfig) throws Exception {
        try {
            final HiveConf conf = getDefaultConf();
            metacatConnectorConfig.getConfiguration().forEach(conf::set);
            DataSourceManager.get().load(metacatConnectorConfig.getCatalogName(),
                metacatConnectorConfig.getConfiguration());
            return new EmbeddedHiveClient(metacatConnectorConfig.getCatalogName(),
                HMSHandlerProxy.getProxy(conf), metacatConnectorConfig.getRegistry());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Failed creating the hive metastore client for catalog: %s",
                    metacatConnectorConfig.getCatalogName()), e);
        }
    }

    /**
     * hive DataSource.
     *
     * @param metacatConnectorConfig connector config.
     * @return data source
     */
    @Bean
    @ConditionalOnProperty(value = "useHiveFastService", havingValue = "true")
    public DataSource hiveDataSource(final MetacatConnectorConfig metacatConnectorConfig) {
        final HiveConf conf = getDefaultConf();
        metacatConnectorConfig.getConfiguration().forEach(conf::set);
        DataSourceManager.get().load(metacatConnectorConfig.getCatalogName(),
            metacatConnectorConfig.getConfiguration());
        return DataSourceManager.get().get(metacatConnectorConfig.getCatalogName());
    }

    private static HiveConf getDefaultConf() {
        final HiveConf result = new HiveConf();
        result.setBoolean(HiveConfigConstants.USE_METASTORE_LOCAL, true);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTORETIMEOUT, 60000);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTOREREADTIMEOUT, 60000);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTOREWRITETIMEOUT, 60000);
        result.setInt(HiveConfigConstants.HIVE_METASTORE_DS_RETRY, 0);
        result.setInt(HiveConfigConstants.HIVE_HMSHANDLER_RETRY, 0);
        result.set(HiveConfigConstants.JAVAX_JDO_PERSISTENCEMANAGER_FACTORY_CLASS,
            HiveConfigConstants.JAVAX_JDO_PERSISTENCEMANAGER_FACTORY);
        result.setBoolean(HiveConfigConstants.HIVE_STATS_AUTOGATHER, false);
        return result;
    }

    /**
     * create thrift hive client.
     *
     * @param metacatConnectorConfig connector config.
     * @return data source
     * @throws MetaException meta exception
     */
    @Bean
    @ConditionalOnProperty(value = "useLocalMetastore", havingValue = "false")
    public IMetacatHiveClient createThriftClient(
        final MetacatConnectorConfig metacatConnectorConfig) throws MetaException {
        final HiveMetastoreClientFactory factory =
            new HiveMetastoreClientFactory(null,
                (int) HiveConnectorUtil.toTime(
                    metacatConnectorConfig.getConfiguration()
                        .getOrDefault(HiveConfigConstants.HIVE_METASTORE_TIMEOUT, "20s"),
                    TimeUnit.SECONDS, TimeUnit.MILLISECONDS));
        final String metastoreUri = metacatConnectorConfig.getConfiguration().get(HiveConfigConstants.THRIFT_URI);
        URI uri = null;
        try {
            uri = new URI(metastoreUri);
        } catch (Exception e) {
            final String message = String.format("Invalid thrift uri %s", metastoreUri);
            log.info(message);
            throw new IllegalArgumentException(message, e);
        }
        return new MetacatHiveClient(uri, factory);
    }

    /**
     * thread Service Manager.
     *
     * @param metacatConnectorConfig connector config
     * @return threadServiceManager
     */
    @Bean
    public ThreadServiceManager threadServiceManager(
        final MetacatConnectorConfig metacatConnectorConfig
    ) {
        return new ThreadServiceManager(metacatConnectorConfig.getConfig());
    }
}
