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
import com.netflix.metacat.common.server.connectors.util.TimeUtil;
import com.netflix.metacat.connector.druid.DruidConfigConstants;
import com.netflix.metacat.connector.druid.MetacatDruidClient;
import com.netflix.metacat.connector.druid.client.DruidHttpClientImpl;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * DruidHttpClientConfig.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Configuration
public class DruidHttpClientConfig {
    /**
     * Druid client instance.
     *
     * @param connectorContext connector context
     * @param restTemplate     rest template
     * @return MetacatDruidClient
     * @throws UnknownHostException exception for unknownhost
     */
    @Bean
    public MetacatDruidClient createMetacatDruidClient(
        final ConnectorContext connectorContext,
        final RestTemplate restTemplate) throws UnknownHostException {
        return new DruidHttpClientImpl(connectorContext, restTemplate);
    }

    /**
     * Rest template.
     *
     * @param connectorContext connector context
     * @return RestTemplate
     */
    @Bean
    public RestTemplate restTemplate(final ConnectorContext connectorContext) {
        return new RestTemplate(new HttpComponentsClientHttpRequestFactory(httpClient(connectorContext)));
    }

    /**
     * Http client.
     *
     * @param connectorContext connector context
     * @return HttpClient
     */
    @Bean
    public HttpClient httpClient(final ConnectorContext connectorContext) {
        final int timeout = (int) TimeUtil.toTime(
            connectorContext.getConfiguration().getOrDefault(DruidConfigConstants.HTTP_TIMEOUT, "5s"),
            TimeUnit.SECONDS,
            TimeUnit.MILLISECONDS
        );
        final int poolsize = Integer.parseInt(connectorContext.getConfiguration()
            .getOrDefault(DruidConfigConstants.POOL_SIZE, "10"));
        final RequestConfig config = RequestConfig.custom()
            .setConnectTimeout(timeout)
            .setConnectionRequestTimeout(timeout)
            .setSocketTimeout(timeout)
            .setMaxRedirects(3)
            .build();
        final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(poolsize);
        return HttpClientBuilder
            .create()
            .setDefaultRequestConfig(config)
            .setConnectionManager(connectionManager)
            .build();
    }
}
