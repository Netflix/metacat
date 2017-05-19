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

import com.netflix.metacat.common.api.MetacatV1;
import com.netflix.metacat.common.api.PartitionV1;
import com.netflix.metacat.common.server.converter.TypeConverterFactory;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.main.services.MetacatThriftService;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.thrift.CatalogThriftServiceFactory;
import com.netflix.metacat.thrift.CatalogThriftServiceFactoryImpl;
import com.netflix.metacat.thrift.DateConverters;
import com.netflix.metacat.thrift.HiveConverters;
import com.netflix.metacat.thrift.HiveConvertersImpl;
import com.netflix.spectator.api.Registry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Configuration for the Thrift Module.
 *
 * @author tgianos
 * @since 1.1.0
 */
@Configuration
public class ThriftConfig {

    /**
     * The hive converters implementation to use.
     *
     * @return The hive converters
     */
    @Bean
    public HiveConverters hiveConverters() {
        return new HiveConvertersImpl();
    }

    /**
     * The Catalog Thrift Service Factory.
     *
     * @param config               Application config to use
     * @param typeConverterFactory Type converters factory to use
     * @param hiveConverters       Hive converters to use
     * @param metacatV1            The Metacat V1 API implementation to use
     * @param partitionV1          The Metacat Partition V1 API to use
     * @param registry             registry for spectator
     * @return The CatalogThriftServiceFactory
     */
    @Bean
    public CatalogThriftServiceFactory catalogThriftServiceFactory(
        final Config config,
        final TypeConverterFactory typeConverterFactory,
        final HiveConverters hiveConverters,
        final MetacatV1 metacatV1,
        final PartitionV1 partitionV1,
        final Registry registry
    ) {
        return new CatalogThriftServiceFactoryImpl(
            config,
            typeConverterFactory,
            hiveConverters,
            metacatV1,
            partitionV1,
            registry
        );
    }

    /**
     * The date converter utility bean.
     *
     * @param config System configuration
     * @return The date converters bean to use
     */
    //TODO: Not sure if this is needed doesn't seem to be being used
    @Bean
    public DateConverters dateConverters(final Config config) {
        return new DateConverters(config);
    }

    /**
     * The MetacatThriftService.
     *
     * @param catalogThriftServiceFactory The factory to use
     * @param connectorManager            The connector manager to use
     * @param registry                    registry for spectator
     * @return The service bean
     */
    @Bean
    public MetacatThriftService metacatThriftService(
        final CatalogThriftServiceFactory catalogThriftServiceFactory,
        final ConnectorManager connectorManager,
        final Registry registry
    ) {
        return new MetacatThriftService(catalogThriftServiceFactory, connectorManager, registry);
    }
}
