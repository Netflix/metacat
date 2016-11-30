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

package com.netflix.metacat.main.search

import com.netflix.metacat.common.MetacatRequestContext
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.Config
import com.netflix.metacat.main.services.search.ElasticSearchMigrationUtil
import com.netflix.metacat.main.services.search.ElasticSearchUtil
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import spock.lang.Shared
import spock.lang.Specification

/**
 * Created by amajumdar on 8/17/15.
 */
class BaseEsSpec extends Specification {
    @Shared
    Config config = Mock(Config)
    @Shared
    ElasticSearchUtil es
    @Shared
    ElasticSearchMigrationUtil esMig
    @Shared
    MetacatJson metacatJson
    @Shared
    MetacatRequestContext metacatContext = new MetacatRequestContext("test", "testApp", "testClientId", "testJobId", null)
    @Shared
    String esIndex = "metacat"
    @Shared
    String esMergeIndex = "meatacat_v2"

    def setupSpec() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("node.http.enabled", false)
                .put("index.gateway.type", "none")
                .put("index.store.type", "memory")
                .put("index.refresh_interval", "1s")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0).build();
        Client client = org.elasticsearch.node.NodeBuilder.nodeBuilder().local(true).settings(settings).node().client()
        String[] indices = [esIndex, esMergeIndex];
        for (String _index : indices) {
            if (client.admin().indices().exists(new IndicesExistsRequest(_index)).actionGet().exists) {
                client.admin().indices().delete(new DeleteIndexRequest(_index)).actionGet()
            }
        }
        // Create a new index
        for (String _index : indices ) {
            def index = new CreateIndexRequest(_index)
            index.source(getFile('metacat.json').getText())
            client.admin().indices().create( index).actionGet()
        }

        metacatJson = MetacatJsonLocator.INSTANCE
        config.getEsIndex() >> esIndex
        config.getWriteEsIndex() >> esMergeIndex
        es = new ElasticSearchUtil(client, config, metacatJson)
        esMig = new ElasticSearchMigrationUtil(client, config, metacatJson);
    }

    def getFile(String name){
        def f = new File('../metacat-main/src/test/resources/search/mapping/' + name)
        if(!f.exists()){
            f = new File('metacat-main/src/test/resources/search/mapping/' + name)
        }
        return f
    }
}
