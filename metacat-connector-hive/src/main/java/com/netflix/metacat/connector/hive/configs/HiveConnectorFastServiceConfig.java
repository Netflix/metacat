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
import com.netflix.metacat.connector.hive.HiveConnectorFastPartitionService;
import com.netflix.metacat.connector.hive.HiveConnectorFastTableService;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * HiveConnectorFastServiceConfig.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Configuration
@ConditionalOnProperty(value = "useHiveFastService", havingValue = "true")
public class HiveConnectorFastServiceConfig {

    /**
     * create hive connector fast partition service.
     *
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @param threadServiceManager thread service manager
     * @param connectorContext      connector config
     * @param dataSource           data source
     * @return HiveConnectorPartitionService
     */
    @Bean
    public HiveConnectorFastPartitionService fastHivePartitionService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter,
        final ThreadServiceManager threadServiceManager,
        final ConnectorContext connectorContext,
        @Qualifier("hiveDataSource") final DataSource dataSource
    ) {
        return new HiveConnectorFastPartitionService(connectorContext.getCatalogName(),
            metacatHiveClient, hiveMetacatConverter,
            connectorContext, threadServiceManager,
            dataSource);
    }

    /**
     * create hive connector fast table service.
     *
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param connectorContext              server context
     * @param dataSource                   data source
     * @return HiveConnectorFastTableService
     */
    @Bean
    public HiveConnectorFastTableService fastHiveTableService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final ConnectorContext connectorContext,
        @Qualifier("hiveDataSource") final DataSource dataSource

    ) {
        return new HiveConnectorFastTableService(
            connectorContext.getCatalogName(), metacatHiveClient,
            hiveConnectorDatabaseService, hiveMetacatConverters,
            Boolean.parseBoolean(
                connectorContext.getConfiguration()
                    .getOrDefault(HiveConfigConstants.ALLOW_RENAME_TABLE, "false")),
            connectorContext,
            dataSource);
    }

}
