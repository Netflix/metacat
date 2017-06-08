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
import com.netflix.metacat.common.server.util.MetacatConnectorProperties;
import com.netflix.metacat.connector.hive.client.DefaultNoOpHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.metacat.connector.hive.util.JdbcUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Hive configs.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Configuration
public class HiveConnectorConfig {

    /**
     * create hive connector partition service.
     *
     * @param catalogName          catelog name
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @return HiveConnectorPartitionService
     */
    @Bean(name = "hivePartitionService")
    @Scope("prototype")
    public HiveConnectorPartitionService partitionService(
        @Value("testhive") final String catalogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter
    ) {
        return new HiveConnectorPartitionService(catalogName, metacatHiveClient, hiveMetacatConverter);
    }

    /**
     * create hive connector fast partition service.
     *
     * @param catalogName          catelog name
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @param jdbcUtil                     jdbc util
     * @param context              context
     * @return HiveConnectorPartitionService
     */
    @Bean(name = "fastHivePartitionService")
    @Scope("prototype")
    public HiveConnectorFastPartitionService fastHivePartitionService(
        @Value("testhive") final String catalogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter,
        final JdbcUtil jdbcUtil,
        final MetacatConnectorProperties context
    ) {
        return new HiveConnectorFastPartitionService(catalogName,
            metacatHiveClient, hiveMetacatConverter,
            jdbcUtil, context);
    }

    /**
     * create hive connector database service.
     *
     * @param catalogName          catelog name
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @return HiveConnectorDatabaseService
     */
    @Bean
    @Scope("prototype")
    public HiveConnectorDatabaseService hiveDatabaseService(@Value("testhive") final String catalogName,
                                                            final IMetacatHiveClient metacatHiveClient,
                                                            final HiveConnectorInfoConverter hiveMetacatConverter) {
        return new HiveConnectorDatabaseService(catalogName, metacatHiveClient, hiveMetacatConverter);
    }

    /**
     * create hive connector table service.
     *
     * @param catalogName                  catelog name
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param allowRenameTable             allow rename table flag
     * @return HiveConnectorTableService
     */
    @Bean({"hiveTableService"})
    @Scope("prototype")
    public HiveConnectorTableService hiveTableService(@Value("testhive") final String catalogName,
                                                      final IMetacatHiveClient metacatHiveClient,
                                                      final HiveConnectorInfoConverter hiveMetacatConverters,
                                                      final HiveConnectorDatabaseService hiveConnectorDatabaseService,
                                                      @Value("false") final boolean allowRenameTable) {
        return new HiveConnectorTableService(catalogName, metacatHiveClient,
            hiveConnectorDatabaseService, hiveMetacatConverters, allowRenameTable);
    }

    /**
     * create hive connector fast table service.
     *
     * @param catalogName                  catelog name
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param jdbcUtil                     jdbc util
     * @param allowRenameTable             allow rename table flag
     * @param metacatConnectorProperties                server context
     * @return HiveConnectorFastTableService
     */
    @Bean(name = "fastHiveTableService")
    @Scope("prototype")
    public HiveConnectorFastTableService fastHiveTableService(
        @Value("testhive") final String catalogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final JdbcUtil jdbcUtil,
        @Value("false") final boolean allowRenameTable,
        final MetacatConnectorProperties metacatConnectorProperties

    ) {
        return new HiveConnectorFastTableService(catalogName, metacatHiveClient,
            hiveConnectorDatabaseService, hiveMetacatConverters,
            jdbcUtil, allowRenameTable, metacatConnectorProperties);
    }

    /**
     * jdbc Util.
     *
     * @param catalogName   catelog Name
     * @param metacatConnectorProperties server Context
     * @return jdbc util
     */
    @Bean
    @Scope("prototype")
    public JdbcUtil createJdbcUtil(
        @Value("testhive") final String catalogName,
        final MetacatConnectorProperties metacatConnectorProperties) {
        DataSourceManager.get().load(catalogName, metacatConnectorProperties.getConfiguration());
        return new JdbcUtil(catalogName, DataSourceManager.get().get(catalogName));
    }

    /**
     * create hive info converter.
     *
     * @return HiveConnectorInfoConverter
     */
    @Bean
    public HiveConnectorInfoConverter hiveConnectorInfoConverter() {
        return new HiveConnectorInfoConverter(new HiveTypeConverter());
    }

    /**
     * create hive client.
     *
     * @return IMetacatHiveClient
     */
    @Bean
    public IMetacatHiveClient hiveClient(
    ) {
        return new DefaultNoOpHiveClient();
    }
}
