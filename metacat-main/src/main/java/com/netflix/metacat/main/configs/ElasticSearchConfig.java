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

import com.google.common.base.Splitter;
import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import com.netflix.metacat.main.services.search.ElasticSearchMetacatRefresh;
import com.netflix.metacat.main.services.search.ElasticSearchUtil;
import com.netflix.metacat.main.services.search.ElasticSearchUtilImpl;
import com.netflix.metacat.main.services.search.MetacatEventHandlers;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for ElasticSearch which triggers when metacat.elasticsearch.enabled is true.
 *
 * @author tgianos
 * @since 1.1.0
 */
//TODO: Look into spring data elasticsearch to replace this
@Configuration
@ConditionalOnProperty(value = "metacat.elasticsearch.enabled", havingValue = "true")
public class ElasticSearchConfig {

    /**
     * The ElasticSearch client.
     *
     * @param config System config
     * @return Configured client or error
     */
    @Bean
    public Client elasticSearchClient(final Config config) {
        final String clusterName = config.getElasticSearchClusterName();
        if (StringUtils.isBlank(clusterName)) {
            throw new IllegalStateException("No cluster name set. Unable to continue");
        }
        final Settings settings = ImmutableSettings
            .settingsBuilder()
            .put("cluster.name", clusterName)
            .put("transport.tcp.connect_timeout", "60s")
            .build();
        final Client client = new TransportClient(settings);
        // Add the transport address if exists
        final String clusterNodesStr = config.getElasticSearchClusterNodes();
        if (StringUtils.isNotBlank(clusterNodesStr)) {
            final int port = config.getElasticSearchClusterPort();
            final Iterable<String> clusterNodes = Splitter.on(',').split(clusterNodesStr);
            clusterNodes.
                forEach(
                    clusterNode ->
                        ((TransportClient) client).addTransportAddress(
                            new InetSocketTransportAddress(clusterNode, port)
                        )
                );
        }

        return client;
    }

    /**
     * ElasticSearch utility wrapper.
     *
     * @param client      The configured ElasticSearch client
     * @param config      System config
     * @param metacatJson JSON utilities
     * @return The ElasticSearch utility instance
     */
    @Bean
    public ElasticSearchUtil elasticSearchUtil(
        final Client client,
        final Config config,
        final MetacatJson metacatJson
    ) {
        return new ElasticSearchUtilImpl(client, config, metacatJson);
    }

    /**
     * Event handler instance to publish event payloads to ElasticSearch.
     *
     * @param elasticSearchUtil The client wrapper utility to use
     * @return The event handler instance
     */
    @Bean
    public MetacatEventHandlers metacatEventHandlers(final ElasticSearchUtil elasticSearchUtil) {
        return new MetacatEventHandlers(elasticSearchUtil);
    }

    /**
     * The refresher of ElasticSearch.
     *
     * @param config              System config
     * @param eventBus            Event bus
     * @param catalogService      Catalog service
     * @param databaseService     Database service
     * @param tableService        Table service
     * @param partitionService    Partition service
     * @param userMetadataService User metadata service
     * @param tagService          Tag service
     * @param elasticSearchUtil   ElasticSearch client wrapper
     * @return The refresh bean
     */
    @Bean
    public ElasticSearchMetacatRefresh elasticSearchMetacatRefresh(
        final Config config,
        final MetacatEventBus eventBus,
        final CatalogService catalogService,
        final DatabaseService databaseService,
        final TableService tableService,
        final PartitionService partitionService,
        final UserMetadataService userMetadataService,
        final TagService tagService,
        final ElasticSearchUtil elasticSearchUtil
    ) {
        return new ElasticSearchMetacatRefresh(
            config,
            eventBus,
            catalogService,
            databaseService,
            tableService,
            partitionService,
            userMetadataService,
            tagService,
            elasticSearchUtil
        );
    }
}
