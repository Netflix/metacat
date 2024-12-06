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

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.connector.druid.MetacatDruidClient;
import com.netflix.metacat.connector.druid.client.DruidHttpClientImpl;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * DruidHttpClientConfig.
 *
 * @author zhenl jtuglu
 * @since 1.2.0
 */
@Configuration
public class DruidHttpClientConfig extends BaseDruidHttpClientConfig {
    /**
     * Druid client instance.
     *
     * @param connectorContext connector context
     * @param restTemplate     rest template
     * @return MetacatDruidClient
     */
    @Override
    public MetacatDruidClient createMetacatDruidClient(
        final ConnectorContext connectorContext,
        final RestTemplate restTemplate) {
        return new DruidHttpClientImpl(connectorContext, restTemplate);
    }
}
