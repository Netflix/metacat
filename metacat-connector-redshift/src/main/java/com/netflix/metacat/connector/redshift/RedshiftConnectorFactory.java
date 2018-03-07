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
package com.netflix.metacat.connector.redshift;

import com.google.common.collect.Lists;
import com.netflix.metacat.common.server.connectors.DefaultConnectorFactory;

import java.util.Map;

/**
 * Connector Factory for Redshift.
 *
 * @author tgianos
 * @since 1.0.0
 */
class RedshiftConnectorFactory extends DefaultConnectorFactory {

    /**
     * Constructor.
     *
     * @param name              catalog name
     * @param catalogShardName  catalog shard name
     * @param configuration     catalog configuration
     */
    RedshiftConnectorFactory(
        final String name,
        final String catalogShardName,
        final Map<String, String> configuration
    ) {
        super(name, catalogShardName, Lists.newArrayList(new RedshiftConnectorModule(catalogShardName, configuration)));
    }
}
