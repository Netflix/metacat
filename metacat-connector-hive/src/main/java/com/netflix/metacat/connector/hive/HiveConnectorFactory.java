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

package com.netflix.metacat.connector.hive;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.SpringConnectorFactory;
import com.netflix.metacat.connector.hive.configs.CacheConfig;
import com.netflix.metacat.connector.hive.configs.HiveConnectorClientConfig;
import com.netflix.metacat.connector.hive.configs.HiveConnectorConfig;
import com.netflix.metacat.connector.hive.configs.HiveConnectorFastServiceConfig;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.MapPropertySource;

import java.util.HashMap;
import java.util.Map;

/**
 * HiveConnectorFactory.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public class HiveConnectorFactory extends SpringConnectorFactory {
    /**
     * Constructor.
     *
     * @param infoConverter    hive info converter
     * @param connectorContext connector config
     */
    HiveConnectorFactory(
        final HiveConnectorInfoConverter infoConverter,
        final ConnectorContext connectorContext
    ) {
        super(infoConverter, connectorContext);
        final boolean useLocalMetastore = Boolean.parseBoolean(
            connectorContext.getConfiguration()
                .getOrDefault(HiveConfigConstants.USE_EMBEDDED_METASTORE, "false")
        );
        final boolean useFastHiveService = Boolean.parseBoolean(
            connectorContext.getConfiguration()
                .getOrDefault(HiveConfigConstants.USE_FASTHIVE_SERVICE, "false")
        );

        final Map<String, Object> properties = new HashMap<>();
        properties.put("useHiveFastService", useFastHiveService);
        properties.put("useEmbeddedClient", useLocalMetastore);
        properties.put("metacat.cache.enabled", connectorContext.getConfig().isCacheEnabled());
        super.addEnvProperties(new MapPropertySource("HIVE_CONNECTOR", properties));
        super.registerClazz(HiveConnectorFastServiceConfig.class,
            HiveConnectorClientConfig.class, HiveConnectorConfig.class, CacheConfig.class);
        super.refresh();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorDatabaseService getDatabaseService() {
        return this.ctx.getBean(HiveConnectorDatabaseService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTableService getTableService() {
        return this.ctx.getBean(HiveConnectorTableService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorPartitionService getPartitionService() {
        return this.ctx.getBean(HiveConnectorPartitionService.class);
    }
}
