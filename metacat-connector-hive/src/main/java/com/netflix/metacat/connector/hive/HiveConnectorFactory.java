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

import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.connector.hive.configs.HiveConnectorClientConfig;
import com.netflix.metacat.connector.hive.configs.HiveConnectorConfig;
import com.netflix.metacat.connector.hive.configs.HiveConnectorFastServiceConfig;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * HiveConnectorFactory.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public class HiveConnectorFactory implements ConnectorFactory {
    private final String catalogName;
    private AnnotationConfigApplicationContext ctx;

    /**
     * Constructor.
     *
     * @param catalogName      connector name. Also the catalog name.
     * @param infoConverter    hive info converter
     * @param connectorContext connector config
     */
    HiveConnectorFactory(
        final String catalogName,
        final HiveConnectorInfoConverter infoConverter,
        final ConnectorContext connectorContext
    ) {
        this.catalogName = catalogName;
        final boolean useLocalMetastore = Boolean.parseBoolean(
            connectorContext.getConfiguration().getOrDefault(HiveConfigConstants.USE_EMBEDDED_METASTORE, "false")
        );
        final boolean useFastHiveService = useLocalMetastore && Boolean.parseBoolean(
            connectorContext.getConfiguration()
                .getOrDefault(HiveConfigConstants.USE_FASTHIVE_SERVICE, "false")
        );

        this.ctx = new AnnotationConfigApplicationContext();
        this.ctx.getBeanFactory().registerSingleton("ConnectorContext", connectorContext);
        this.ctx.getBeanFactory().registerSingleton("HiveConnectorInfoConverter", infoConverter);
        final StandardEnvironment standardEnvironment = new StandardEnvironment();
        final MutablePropertySources propertySources = standardEnvironment.getPropertySources();
        final Map<String, Object> properties = new HashMap<>();
        properties.put("useHiveFastService", useFastHiveService);
        properties.put("useThriftClient", !useLocalMetastore);
        propertySources.addFirst(new MapPropertySource("HIVE_CONNECTOR", properties));
        this.ctx.setEnvironment(standardEnvironment);
        //TODO scan the package, which is not working
//        this.ctx.scan(this.getClass().getPackage().getName());
        this.ctx.register(HiveConnectorFastServiceConfig.class);
        this.ctx.register(HiveConnectorClientConfig.class);
        this.ctx.register(HiveConnectorConfig.class);
        this.ctx.refresh();
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

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.catalogName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        this.ctx.close();
    }
}
