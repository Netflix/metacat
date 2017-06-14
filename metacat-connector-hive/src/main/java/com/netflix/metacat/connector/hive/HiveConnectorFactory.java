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
import com.netflix.metacat.common.server.util.MetacatConnectorConfig;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
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
    private IMetacatHiveClient client;
    private HiveConnectorPartitionService partitionService;
    private HiveConnectorTableService tableService;
    private HiveConnectorDatabaseService databaseService;
    private AnnotationConfigApplicationContext ctx;

    /**
     * Constructor.
     *
     * @param catalogName            connector name. Also the catalog name.
     * @param infoConverter          hive info converter
     * @param metacatConnectorConfig connector config
     */
    public HiveConnectorFactory(
        final String catalogName,
        final HiveConnectorInfoConverter infoConverter,
        final MetacatConnectorConfig metacatConnectorConfig
    ) {
        this.catalogName = catalogName;

        final boolean useLocalMetastore = Boolean
            .parseBoolean(metacatConnectorConfig.getConfiguration().getOrDefault(
                HiveConfigConstants.USE_EMBEDDED_METASTORE, "false"));
        boolean useFastHiveService = false;
        if (useLocalMetastore) {
            useFastHiveService = Boolean.parseBoolean(
                metacatConnectorConfig.getConfiguration()
                    .getOrDefault(HiveConfigConstants.USE_FASTHIVE_SERVICE, "false"));
        }

        ctx = new AnnotationConfigApplicationContext();
        ctx.getBeanFactory().registerSingleton("MetacatConnectorConfig", metacatConnectorConfig);
        ctx.getBeanFactory().registerSingleton("HiveConnectorInfoConverter", infoConverter);
        final StandardEnvironment standardEnvironment = new StandardEnvironment();
        final MutablePropertySources propertySources = standardEnvironment.getPropertySources();
        final Map<String, Object> properties = new HashMap<>();
        properties.put("useHiveFastService", String.valueOf(useFastHiveService));
        properties.put("useLocalMetastore", String.valueOf(useLocalMetastore));
        propertySources.addFirst(new MapPropertySource("HIVE_CONNECTOR", properties));
        ctx.setEnvironment(standardEnvironment);
        ctx.register(HiveConnectorConfig.class);
        ctx.refresh();

        this.client = ctx.getBean(IMetacatHiveClient.class);
        this.databaseService = ctx.getBean(HiveConnectorDatabaseService.class);
        this.partitionService = ctx.getBean(HiveConnectorPartitionService.class);
        this.tableService = ctx.getBean(HiveConnectorTableService.class);
    }

    @Override
    public ConnectorDatabaseService getDatabaseService() {
        return this.databaseService;
    }

    @Override
    public ConnectorTableService getTableService() {
        return this.tableService;
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        return this.partitionService;
    }

    @Override
    public String getName() {
        return catalogName;
    }

    @Override
    public void stop() {
        try {
            client.shutdown();
            ctx.close();

        } catch (TException e) {
            log.warn("Failed shutting down the catalog: {}", catalogName, e);
        }
    }
}
