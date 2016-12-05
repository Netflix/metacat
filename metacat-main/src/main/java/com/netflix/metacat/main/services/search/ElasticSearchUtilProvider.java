/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.main.services.search;

import com.netflix.metacat.common.json.MetacatJson;
import com.netflix.metacat.common.server.Config;
import org.elasticsearch.client.Client;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

/**
 * Provider to create the elasticSearchUtil based on condition.
 * If isIndexMigration is true, it will return the ElasticSearchMigrationUtil
 * otherwise return ElasticSearchUtil
 */
public class ElasticSearchUtilProvider implements Provider<ElasticSearchUtil> {
    private Config config;
    private Client client;
    private MetacatJson metacatJson;

    /**
     * Constructor.
     * @param client client
     * @param config config
     * @param metacatJson metacatJson
     */
    @Inject
    public ElasticSearchUtilProvider(
        @Nullable
            final Client client,
        final Config config,
        final MetacatJson metacatJson) {
        this.config = config;
        this.client = client;
        this.metacatJson = metacatJson;
    }

    @Override
    public ElasticSearchUtil get() {
         return config.isIndexMigration() ? new ElasticSearchMigrationUtil(client, config, metacatJson)
            : new ElasticSearchUtil(client, config, metacatJson);
    }
}
