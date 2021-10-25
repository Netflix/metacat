package com.netflix.metacat.connector.polaris;

import com.google.common.collect.Lists;
import com.netflix.metacat.common.server.connectors.DefaultConnectorFactory;

import java.util.Map;

/**
 * Connector Factory for Polaris.
 */
class PolarisConnectorFactory extends DefaultConnectorFactory {

    /**
     * Constructor.
     *
     * @param catalogName       catalog name
     * @param catalogShardName  catalog shard name
     * @param configuration     catalog configuration
     */
    PolarisConnectorFactory(
        final String catalogName,
        final String catalogShardName,
        final Map<String, String> configuration
    ) {
        super(catalogName, catalogShardName,
            Lists.newArrayList(new PolarisConnectorModule(catalogShardName, configuration)));
    }
}
