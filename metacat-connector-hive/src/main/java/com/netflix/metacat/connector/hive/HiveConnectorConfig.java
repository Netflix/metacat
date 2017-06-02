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

import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.properties.DefaultConfigImpl;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.common.server.util.ServerContext;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.client.DefaultNoOpHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.spectator.api.DefaultRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.Collections;

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
     * @param catelogName          catelog name
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @return HiveConnectorPartitionService
     */
    @Bean({"hive-partitionservice"})
    @Scope("prototype")
    public HiveConnectorPartitionService createHivePartitionService(
        @Value("testhive") final String catelogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter
    ) {
        return new HiveConnectorPartitionService(catelogName, metacatHiveClient, hiveMetacatConverter);
    }

    /**
     * create hive connector fast partition service.
     *
     * @param catelogName          catelog name
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @param threadServiceManager threadServiceManager
     * @param context              context
     * @return HiveConnectorPartitionService
     */
    @Bean({"fast-hive-partitionservice"})
    @Scope("prototype")
    public HiveConnectorFastPartitionService createFastHivePartitionService(
        @Value("testhive") final String catelogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverter,
        final ThreadServiceManager threadServiceManager,
        final ServerContext context
    ) {
        return new HiveConnectorFastPartitionService(catelogName,
            metacatHiveClient, hiveMetacatConverter, threadServiceManager,
            context);
    }

    /**
     * create hive connector database service.
     *
     * @param catelogName          catelog name
     * @param metacatHiveClient    hive client
     * @param hiveMetacatConverter metacat converter
     * @return HiveConnectorDatabaseService
     */
    @Bean
    @Scope("prototype")
    public HiveConnectorDatabaseService createDatabaseService(@Value("testhive") final String catelogName,
                                                              final IMetacatHiveClient metacatHiveClient,
                                                              final HiveConnectorInfoConverter hiveMetacatConverter) {
        return new HiveConnectorDatabaseService(catelogName, metacatHiveClient, hiveMetacatConverter);
    }

    /**
     * create hive connector table service.
     *
     * @param catelogName                  catelog name
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param allowRenameTable             allow rename table flag
     * @return HiveConnectorTableService
     */
    @Bean({"hive-tableservice"})
    @Scope("prototype")
    public HiveConnectorTableService createTableService(@Value("testhive") final String catelogName,
                                                        final IMetacatHiveClient metacatHiveClient,
                                                        final HiveConnectorInfoConverter hiveMetacatConverters,
                                                        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
                                                        @Value("false") final boolean allowRenameTable) {
        return new HiveConnectorTableService(catelogName, metacatHiveClient,
            hiveConnectorDatabaseService, hiveMetacatConverters, allowRenameTable);
    }

    /**
     * create hive connector fast table service.
     *
     * @param catelogName                  catelog name
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param allowRenameTable             allow rename table flag
     * @param serverContext                server context
     * @param threadServiceManager         thread service manager
     * @return HiveConnectorFastTableService
     */
    @Bean({"fast-hive-tableservice"})
    @Scope("prototype")
    public HiveConnectorFastTableService createFastTableService(
        @Value("testhive") final String catelogName,
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final ThreadServiceManager threadServiceManager,
        @Value("false") final boolean allowRenameTable,
        final ServerContext serverContext
    ) {
        return new HiveConnectorFastTableService(catelogName, metacatHiveClient,
            hiveConnectorDatabaseService, hiveMetacatConverters, threadServiceManager, allowRenameTable, serverContext);
    }

    /**
     * create hive info converter.
     *
     * @return HiveConnectorInfoConverter
     */
    @Bean
    public HiveConnectorInfoConverter createHiveConverter() {
        return new HiveConnectorInfoConverter(new HiveTypeConverter());
    }

    /**
     * create hive client.
     *
     * @return IMetacatHiveClient
     */
    @Bean
    public IMetacatHiveClient createHiveClient(
    ) {
        return new DefaultNoOpHiveClient();
    }

    /**
     * create server context.
     *
     * @return ServerContext
     */
    @Bean
    public ServerContext getServerContext() {
        return new ServerContext(new DefaultRegistry(), Collections.emptyMap());
    }

    /**
     * get thread service manager.
     *
     * @param config configuration
     * @return Thread Service Manager
     */
    @Bean
    public ThreadServiceManager getthreadServiceManager(
        final Config config
    ) {
        return new ThreadServiceManager(config);
    }

    /**
     * metacat Properties.
     *
     * @return Metacat properties
     */
    @Bean
    @ConditionalOnMissingBean
    @ConfigurationProperties("metacat")
    public MetacatProperties metacatProperties() {
        return new MetacatProperties();
    }

    /**
     * Get the configuration abstraction for use in metacat.
     *
     * @param metacatProperties The overall metacat properties to use
     * @return The configuration object
     */
    @Bean
    @ConditionalOnMissingBean
    public Config config(final MetacatProperties metacatProperties) {
        return new DefaultConfigImpl(metacatProperties);
    }
}
