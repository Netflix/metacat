package com.netflix.metacat.connector.druid.configs;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.util.TimeUtil;
import com.netflix.metacat.connector.druid.DruidConfigConstants;
import com.netflix.metacat.connector.druid.MetacatDruidClient;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.TimeUnit;

/**
 * BaseDruidHttpClientConfig.
 *
 * @author zhenl jtuglu
 * @since 1.2.0
 */
public abstract class BaseDruidHttpClientConfig {
    /**
     * Druid client instance.
     *
     * @param connectorContext connector context
     * @param restTemplate     rest template
     * @return MetacatDruidClient
     */
    @Bean
    public abstract MetacatDruidClient createMetacatDruidClient(
        final ConnectorContext connectorContext,
        final RestTemplate restTemplate
    );

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
        final int poolSize = Integer.parseInt(connectorContext.getConfiguration()
            .getOrDefault(DruidConfigConstants.POOL_SIZE, "10"));
        final RequestConfig config = RequestConfig.custom()
            .setConnectTimeout(timeout)
            .setConnectionRequestTimeout(timeout)
            .setSocketTimeout(timeout)
            .setMaxRedirects(3)
            .build();
        final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(poolSize);
        return HttpClientBuilder
            .create()
            .setDefaultRequestConfig(config)
            .setConnectionManager(connectionManager)
            .build();
    }
}
