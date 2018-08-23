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

import com.netflix.metacat.common.server.converter.ConverterUtil;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.DefaultLookupService;
import com.netflix.metacat.common.server.usermetadata.DefaultTagService;
import com.netflix.metacat.common.server.usermetadata.DefaultUserMetadataService;
import com.netflix.metacat.common.server.usermetadata.LookupService;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.main.manager.CatalogManager;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.main.manager.PluginManager;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.MViewService;
import com.netflix.metacat.main.services.MetacatInitializationService;
import com.netflix.metacat.main.services.MetacatServiceHelper;
import com.netflix.metacat.main.services.MetacatThriftService;
import com.netflix.metacat.main.services.MetadataService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import com.netflix.metacat.main.services.impl.CatalogServiceImpl;
import com.netflix.metacat.main.services.impl.ConnectorTableServiceProxy;
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
     * No-op User Metadata service.
     *
     * @return User metadata service based on MySql
     */
    @Bean
    @ConditionalOnMissingBean(UserMetadataService.class)
    public UserMetadataService userMetadataService() {
        return new DefaultUserMetadataService();
    }

    /**
     * No-op Tag service.
     *
     * @return User metadata service based on MySql
     */
    @Bean
    @ConditionalOnMissingBean(TagService.class)
    public TagService tagService() {
        return new DefaultTagService();
    }

    /**
     * No-op Look up service.
     *
     * @return User metadata service based on MySql
     */
    @Bean
    @ConditionalOnMissingBean(LookupService.class)
    public LookupService lookupService() {
        return new DefaultLookupService();
    }

    /**
     * The catalog service bean.
     *
     * @param connectorManager    Connector manager to use
     * @param userMetadataService  User metadata service
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
     * @param connectorTableServiceProxy   connector table service proxy
     * @param databaseService     database service
     * @param tagService          tag service
     * @param userMetadataService user metadata service
     * @param eventBus            Internal event bus
     * @param registry             registry handle
     * @param config               configurations
     * @return The table service bean
     */
    @Bean
    public TableService tableService(
        final ConnectorTableServiceProxy connectorTableServiceProxy,
        final DatabaseService databaseService,
        final TagService tagService,
        final UserMetadataService userMetadataService,
        final MetacatEventBus eventBus,
        final Registry registry,
        final Config config
    ) {
        return new TableServiceImpl(
            connectorTableServiceProxy,
            databaseService,
            tagService,
            userMetadataService,
            eventBus,
            registry,
            config
        );
    }

    /**
     * The connector table service proxy bean.
     *
     * @param connectorManager    Connector manager to use
     * @param converterUtil       Converter utilities
     * @return The connector table service proxy bean
     */
    @Bean
    public ConnectorTableServiceProxy connectorTableServiceProxy(
        final ConnectorManager connectorManager,
        final ConverterUtil converterUtil
    ) {
        return new ConnectorTableServiceProxy(
            connectorManager,
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
     * @param userMetadataService  user metadata service
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
     * @param mViewService     view service
     * @return The service helper instance to use
     */
    @Bean
    public MetacatServiceHelper metacatServiceHelper(
        final DatabaseService databaseService,
        final TableService tableService,
        final PartitionService partitionService,
        final MViewService mViewService,
        final MetacatEventBus eventBus
    ) {
        return new MetacatServiceHelper(databaseService, tableService, partitionService, mViewService, eventBus);
    }

    /**
     * Metadata service bean.
     *
     * @param config              System config
     * @param tableService        The table service to use
     * @param partitionService    The partition service to use
     * @param userMetadataService The user metadata service to use
     * @param tagService          tag service
     * @param helper              Metacat service helper
     * @param registry            registry for spectator
     * @return The metadata service bean
     */
    @Bean
    public MetadataService metadataService(
        final Config config,
        final TableService tableService,
        final PartitionService partitionService,
        final UserMetadataService userMetadataService,
        final TagService tagService,
        final MetacatServiceHelper helper,
        final Registry registry
    ) {
        return new MetadataService(config, tableService, partitionService, userMetadataService,
            tagService, helper, registry);
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
