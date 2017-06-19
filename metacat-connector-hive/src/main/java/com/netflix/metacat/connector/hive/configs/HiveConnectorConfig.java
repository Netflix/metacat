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

import com.netflix.metacat.common.server.util.ConnectorContext;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService;
import com.netflix.metacat.connector.hive.HiveConnectorPartitionService;
import com.netflix.metacat.connector.hive.HiveConnectorTableService;
import com.netflix.metacat.connector.hive.HiveConnectorUtil;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.client.thrift.HiveMetastoreClientFactory;
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
     * create hive connector database service.
     *
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @param connectorContext      connector config
     * @return HiveConnectorDatabaseService
     */
    @Bean
    public HiveConnectorDatabaseService hiveDatabaseService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter,
        final ConnectorContext connectorContext) {
        return new HiveConnectorDatabaseService(connectorContext.getCatalogName(),
            metacatHiveClient, hiveMetacatConverter);
    }

    /**
     * create hive connector partition service.
     *
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @param connectorContext      connector config
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
            connectorContext.getCatalogName(), metacatHiveClient, hiveMetacatConverter);
    }

    /**
     * create hive connector table service.
     *
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param connectorContext              connector config
     * @return HiveConnectorTableService
     */
    @Bean
    @ConditionalOnMissingBean(HiveConnectorTableService.class)
    public HiveConnectorTableService hiveTableService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final ConnectorContext connectorContext) {
        return new HiveConnectorTableService(connectorContext.getCatalogName(), metacatHiveClient,
            hiveConnectorDatabaseService, hiveMetacatConverters, Boolean.parseBoolean(
            connectorContext.getConfiguration()
                .getOrDefault(HiveConfigConstants.ALLOW_RENAME_TABLE, "false")));
    }


    /**
     * create thrift hive client.
     *
     * @param connectorContext connector config.
     * @return data source
     * @throws MetaException meta exception
     */
    @Bean
  //  @ConditionalOnProperty(value = "useThriftClient", havingValue = "true")
    @ConditionalOnMissingBean(IMetacatHiveClient.class)
    public IMetacatHiveClient createThriftClient(
        final ConnectorContext connectorContext) throws MetaException {
        final HiveMetastoreClientFactory factory =
            new HiveMetastoreClientFactory(null,
                (int) HiveConnectorUtil.toTime(
                    connectorContext.getConfiguration()
                        .getOrDefault(HiveConfigConstants.HIVE_METASTORE_TIMEOUT, "20s"),
                    TimeUnit.SECONDS, TimeUnit.MILLISECONDS));
        final String metastoreUri = connectorContext.getConfiguration().get(HiveConfigConstants.THRIFT_URI);
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
     * @param connectorContext connector config
     * @return threadServiceManager
     */
    @Bean
    public ThreadServiceManager threadServiceManager(
        final ConnectorContext connectorContext
    ) {
        return new ThreadServiceManager(connectorContext.getConfig());
    }
}
