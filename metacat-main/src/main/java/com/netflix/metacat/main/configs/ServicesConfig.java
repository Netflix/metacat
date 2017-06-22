/*
 *
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
 *
 */
package com.netflix.metacat.main.configs;

import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.DefaultLookupService;
import com.netflix.metacat.common.server.usermetadata.DefaultTagService;
import com.netflix.metacat.common.server.usermetadata.DefaultUserMetadataService;
import com.netflix.metacat.common.server.usermetadata.LookupService;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.common.server.manager.CatalogManager;
import com.netflix.metacat.common.server.manager.ConnectorManager;
import com.netflix.metacat.common.server.manager.PluginManager;
import com.netflix.metacat.common.server.services.CatalogService;
import com.netflix.metacat.common.server.services.DatabaseService;
import com.netflix.metacat.common.server.services.MViewService;
import com.netflix.metacat.main.services.MetacatInitializationService;
import com.netflix.metacat.main.services.MetacatServiceHelper;
import com.netflix.metacat.thrift.MetacatThriftService;
import com.netflix.metacat.main.services.MetadataService;
import com.netflix.metacat.common.server.services.PartitionService;
import com.netflix.metacat.common.server.services.TableService;
import com.netflix.metacat.main.services.impl.CatalogServiceImpl;
import com.netflix.metacat.main.services.impl.DatabaseServiceImpl;
import com.netflix.metacat.main.services.impl.MViewServiceImpl;
import com.netflix.metacat.main.services.impl.PartitionServiceImpl;
import com.netflix.metacat.main.services.impl.TableServiceImpl;
import com.netflix.spectator.api.Registry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration of Service Tier.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
public class ServicesConfig {

    /**
     * User Metadata service.
     *
     * @param dataSourceManager The datasource manager to use
     * @param config            System config to use
     * @param metacatJson       Json Utilities to use
     * @return User metadata service based on MySql
     */
    @Bean
    @ConditionalOnMissingBean
    public UserMetadataService userMetadataService(
        final DataSourceManager dataSourceManager,
        final Config config,
        final MetacatJson metacatJson
    ) {
        return new DefaultUserMetadataService();
    }

    /**
     * Tag service.
     *
     * @param dataSourceManager The datasource manager to use
     * @param config            System config to use
     * @param metacatJson       Json Utilities to use
     * @return User metadata service based on MySql
     */
    @Bean
    @ConditionalOnMissingBean
    public TagService tagService(
        final DataSourceManager dataSourceManager,
        final Config config,
        final MetacatJson metacatJson
    ) {
        return new DefaultTagService();
    }

    /**
     * Look up service.
     *
     * @param dataSourceManager The datasource manager to use
     * @param config            System config to use
     * @param metacatJson       Json Utilities to use
     * @return User metadata service based on MySql
     */
    @Bean
    @ConditionalOnMissingBean
    public LookupService lookupService(
        final DataSourceManager dataSourceManager,
        final Config config,
        final MetacatJson metacatJson
    ) {
        return new DefaultLookupService();
    }

    /**
     * The catalog service bean.
     *
     * @param connectorManager    Connector manager to use
     * @param userMetadataService User metadata service to use
     * @param metacatEventBus     Event bus to use
     * @param converterUtil       Converter utilities
     * @return Catalog service implementation
     */
    @Bean
    public CatalogService catalogService(
        final ConnectorManager connectorManager,
        final UserMetadataService userMetadataService,
        final MetacatEventBus metacatEventBus,
        final ConverterUtil converterUtil
    ) {
        return new CatalogServiceImpl(connectorManager, userMetadataService, metacatEventBus, converterUtil);
    }

    /**
     * The database service bean.
     *
     * @param connectorManager    Connector manager to use
     * @param userMetadataService User metadata service to use
     * @param metacatEventBus     Event bus to use
     * @param converterUtil       Converter utilities
     * @param catalogService      The catalog service to use
     * @return Catalog service implementation
     */
    @Bean
    public DatabaseService databaseService(
        final ConnectorManager connectorManager,
        final UserMetadataService userMetadataService,
        final MetacatEventBus metacatEventBus,
        final ConverterUtil converterUtil,
        final CatalogService catalogService
    ) {
        return new DatabaseServiceImpl(
            catalogService,
            connectorManager,
            userMetadataService,
            metacatEventBus,
            converterUtil
        );
    }

    /**
     * The table service bean.
     *
     * @param connectorManager    connector manager
     * @param databaseService     database service
     * @param tagService          tag service
     * @param userMetadataService user metadata service
     * @param eventBus            Internal event bus
     * @param converterUtil       utility to convert to/from Dto to connector resources
     * @return The table service bean
     */
    @Bean
    public TableService tableService(
        final ConnectorManager connectorManager,
        final DatabaseService databaseService,
        final TagService tagService,
        final UserMetadataService userMetadataService,
        final MetacatEventBus eventBus,
        final ConverterUtil converterUtil
    ) {
        return new TableServiceImpl(
            connectorManager,
            databaseService,
            tagService,
            userMetadataService,
            eventBus,
            converterUtil
        );
    }

    /**
     * Partition service bean.
     *
     * @param catalogService       catalog service
     * @param connectorManager     connector manager
     * @param tableService         table service
     * @param userMetadataService  user metadata service
     * @param threadServiceManager thread manager
     * @param config               configurations
     * @param eventBus             Internal event bus
     * @param converterUtil        utility to convert to/from Dto to connector resources
     * @param registry             registry handle
     * @return The partition service implementation to use
     */
    @Bean
    public PartitionService partitionService(
        final CatalogService catalogService,
        final ConnectorManager connectorManager,
        final TableService tableService,
        final UserMetadataService userMetadataService,
        final ThreadServiceManager threadServiceManager,
        final Config config,
        final MetacatEventBus eventBus,
        final ConverterUtil converterUtil,
        final Registry registry
    ) {
        return new PartitionServiceImpl(
            catalogService,
            connectorManager,
            tableService,
            userMetadataService,
            threadServiceManager,
            config,
            eventBus,
            converterUtil,
            registry
        );
    }

    /**
     * The MViewService bean.
     *
     * @param connectorManager    connector manager
     * @param tableService        table service
     * @param partitionService    partition service
     * @param userMetadataService user metadata service
     * @param eventBus            Internal event bus
     * @param converterUtil       utility to convert to/from Dto to connector resources
     * @return The MViewService implementation to use
     */
    @Bean
    public MViewService mViewService(
        final ConnectorManager connectorManager,
        final TableService tableService,
        final PartitionService partitionService,
        final UserMetadataService userMetadataService,
        final MetacatEventBus eventBus,
        final ConverterUtil converterUtil
    ) {
        return new MViewServiceImpl(
            connectorManager,
            tableService,
            partitionService,
            userMetadataService,
            eventBus,
            converterUtil
        );
    }

    /**
     * The service helper.
     *
     * @param databaseService  database service
     * @param tableService     table service
     * @param partitionService partition service
     * @param eventBus         event bus
     * @return The service helper instance to use
     */
    @Bean
    public MetacatServiceHelper metacatServiceHelper(
        final DatabaseService databaseService,
        final TableService tableService,
        final PartitionService partitionService,
        final MetacatEventBus eventBus
    ) {
        return new MetacatServiceHelper(databaseService, tableService, partitionService, eventBus);
    }

    /**
     * Metadata service bean.
     *
     * @param config              System config
     * @param tableService        The table service to use
     * @param partitionService    The partition service to use
     * @param userMetadataService The user metadata service to use
     * @param registry            registry for spectator
     * @return The metadata service bean
     */
    @Bean
    public MetadataService metadataService(
        final Config config,
        final TableService tableService,
        final PartitionService partitionService,
        final UserMetadataService userMetadataService,
        final Registry registry
    ) {
        return new MetadataService(config, tableService, partitionService, userMetadataService, registry);
    }

    /**
     * The initialization service that will handle startup and shutdown of Metacat.
     *
     * @param pluginManager        Plugin manager to use
     * @param catalogManager       Catalog manager to use
     * @param connectorManager     Connector manager to use
     * @param threadServiceManager Thread service manager to use
     * @param metacatThriftService Thrift service to use
     * @return The initialization service bean
     */
    @Bean
    public MetacatInitializationService metacatInitializationService(
        final PluginManager pluginManager,
        final CatalogManager catalogManager,
        final ConnectorManager connectorManager,
        final ThreadServiceManager threadServiceManager,
        final MetacatThriftService metacatThriftService
    ) {
        return new MetacatInitializationService(
            pluginManager,
            catalogManager,
            connectorManager,
            threadServiceManager,
            metacatThriftService
        );
    }
}
