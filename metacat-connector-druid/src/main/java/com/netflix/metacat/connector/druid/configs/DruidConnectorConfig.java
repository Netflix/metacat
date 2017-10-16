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

package com.netflix.metacat.connector.druid.configs;

import com.netflix.metacat.connector.druid.DruidConnectorDatabaseService;
import com.netflix.metacat.connector.druid.DruidConnectorPartitionService;
import com.netflix.metacat.connector.druid.DruidConnectorTableService;
import com.netflix.metacat.connector.druid.MetacatDruidClient;
import com.netflix.metacat.connector.druid.converter.DruidConnectorInfoConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Druid Connector Config.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Configuration
public class DruidConnectorConfig {
    /**
     * create druid connector table service.
     *
     * @param druidClient        druid Client
     * @param druidConnectorInfoConverter druid info converter
     * @return druid connector table Service
     */
    @Bean
    public DruidConnectorTableService druidTableService(
        final MetacatDruidClient druidClient,
        final DruidConnectorInfoConverter druidConnectorInfoConverter) {
        return new DruidConnectorTableService(
            druidClient,
            druidConnectorInfoConverter
        );
    }

    /**
     * create druid connector database service.
     *
     * @return druid connector database Service
     */
    @Bean
    public DruidConnectorDatabaseService druidDatabaseService() {
        return new DruidConnectorDatabaseService();
    }

    /**
     * create druid connector partition service.
     *
     * @param druidClient druid Client
     * @param druidConnectorInfoConverter druid info converter
     * @return druid connector partition Service
     */
    @Bean
    public DruidConnectorPartitionService druidPartitionService(
        final MetacatDruidClient druidClient,
        final DruidConnectorInfoConverter druidConnectorInfoConverter) {
        return new DruidConnectorPartitionService(druidClient, druidConnectorInfoConverter);
    }

}
