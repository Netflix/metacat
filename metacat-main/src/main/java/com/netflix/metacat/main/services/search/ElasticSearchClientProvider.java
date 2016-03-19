package com.netflix.metacat.main.services.search;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.netflix.metacat.common.server.Config;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import javax.inject.Inject;
import javax.inject.Provider;

public class ElasticSearchClientProvider implements Provider<Client> {
    private Client client;

    @Inject
    public ElasticSearchClientProvider(Config config) {
        String clusterName = config.getElasticSearchClusterName();
        if( clusterName != null ) {
            Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
                    .put("transport.tcp.connect_timeout", "60s").build();
            client = new TransportClient(settings);
            // Add the transport address if exists
            String clusterNodesStr = config.getElasticSearchClusterNodes();
            if(!Strings.isNullOrEmpty( clusterNodesStr)){
                Iterable<String> clusterNodes = Splitter.on(',').split(clusterNodesStr);
                clusterNodes.forEach( clusterNode -> ((TransportClient)client).addTransportAddress( new InetSocketTransportAddress( clusterNode, config.getElasticSearchClusterPort())));
            }
        }
    }

    @Override
    public Client get() {
        return client;
    }
}
