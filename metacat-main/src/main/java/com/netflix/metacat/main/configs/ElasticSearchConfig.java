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
import com.netflix.metacat.common.server.events.MetacatEventBus;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.usermetadata.TagService;
import com.netflix.metacat.common.server.usermetadata.UserMetadataService;
import com.netflix.metacat.main.services.CatalogService;
import com.netflix.metacat.main.services.DatabaseService;
import com.netflix.metacat.main.services.PartitionService;
import com.netflix.metacat.main.services.TableService;
import com.netflix.metacat.main.services.search.ElasticSearchCatalogTraversalAction;
import com.netflix.metacat.main.services.search.ElasticSearchEventHandlers;
import com.netflix.metacat.main.services.search.ElasticSearchRefresh;
import com.netflix.metacat.main.services.search.ElasticSearchUtil;
import com.netflix.metacat.main.services.search.ElasticSearchUtilImpl;
import com.netflix.spectator.api.Registry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.common.transport.TransportAddress;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

/**
 * Configuration for ElasticSearch which triggers when
 * metacat.elasticsearch.enabled is true.
 *
 * @author tgianos
 * @author zhenl
 * @since 1.1.0
 */
@Slf4j
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
    @ConditionalOnMissingBean(Client.class)
    public Client elasticSearchClient(final Config config) {
        try {
            String clusterName = config.getElasticSearchClusterName();
            if (StringUtils.isBlank(clusterName)) {
                clusterName = "elasticsearch";
            }
            final Settings settings = Settings.builder().put("cluster.name", clusterName)
                .put("client.transport.sniff", false) // to dynamically add new hosts and remove old ones
                .put("transport.tcp.connect_timeout", "60s").put("client.transport.ignore_cluster_name", true).build();

            final TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9200));
            return client;
        } catch (Exception exc) {
            throw new RuntimeException(exc);
        }
    }

    /**
     * ElasticSearch utility wrapper.
     *
     * @param client      The configured ElasticSearch client
     * @param config      System config
     * @param metacatJson JSON utilities
     * @param registry    spectator registry
     * @return The ElasticSearch utility instance
     */
    @Bean
    public ElasticSearchUtil elasticSearchUtil(final Client client, final Config config, final MetacatJson metacatJson,
            final Registry registry) {
        return new ElasticSearchUtilImpl(client, config, metacatJson, registry);
    }

    /**
     * Event handler instance to publish event payloads to ElasticSearch.
     *
     * @param elasticSearchUtil The client wrapper utility to use
     * @param registry          registry of spectator
     * @param config            System config
     * @return The event handler instance
     */
    @Bean
    public ElasticSearchEventHandlers elasticSearchEventHandlers(final ElasticSearchUtil elasticSearchUtil,
            final Registry registry, final Config config) {
        return new ElasticSearchEventHandlers(elasticSearchUtil, registry, config);
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
     * @param registry            registry of spectator
     * @return The refresh bean
     */
    @Bean
    public ElasticSearchRefresh elasticSearchRefresh(final Config config, final MetacatEventBus eventBus,
            final CatalogService catalogService, final DatabaseService databaseService, final TableService tableService,
            final PartitionService partitionService, final UserMetadataService userMetadataService,
            final TagService tagService, final ElasticSearchUtil elasticSearchUtil, final Registry registry) {
        return new ElasticSearchRefresh(config, eventBus, catalogService, databaseService, tableService,
                partitionService, userMetadataService, tagService, elasticSearchUtil, registry);
    }

    /**
     * Traversal action implementation for ElasticSearch refresh.
     *
     * @param config              System config
     * @param eventBus            Event bus
     * @param databaseService     Database service
     * @param tableService        Table service
     * @param userMetadataService User metadata service
     * @param tagService          Tag service
     * @param elasticSearchUtil   ElasticSearch client wrapper
     * @param registry            registry of spectator
     * @return The refresh bean
     */
    @Bean
    public ElasticSearchCatalogTraversalAction elasticSearchCatalogTraversalAction(final Config config,
            final MetacatEventBus eventBus, final DatabaseService databaseService, final TableService tableService,
            final UserMetadataService userMetadataService, final TagService tagService,
            final ElasticSearchUtil elasticSearchUtil, final Registry registry) {
        return new ElasticSearchCatalogTraversalAction(config, eventBus, databaseService, tableService,
                userMetadataService, tagService, elasticSearchUtil, registry);
    }
}
