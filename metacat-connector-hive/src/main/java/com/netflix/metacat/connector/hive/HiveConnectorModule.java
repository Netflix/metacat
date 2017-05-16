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

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.properties.DefaultConfigImpl;
import com.netflix.metacat.common.server.properties.MetacatProperties;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import com.netflix.spectator.api.Registry;

import java.util.Map;

/**
 * HiveConnectorModule.
 *
 * @author zhenl
 * @since 1.0.0
 */
public class HiveConnectorModule extends AbstractModule {
    private final String catalogName;
    private final HiveConnectorInfoConverter infoConverter;
    private final IMetacatHiveClient hiveMetastoreClient;
    private final boolean fastService;
    private final boolean allowRenameTable;
    private final Registry registry;
    /**
     * Constructor.
     *
     * @param catalogName         catalog name.
     * @param configuration       configuration properties
     * @param infoConverter       Hive info converter
     * @param hiveMetastoreClient hive metastore client
     */
    HiveConnectorModule(
        final String catalogName,
        final Map<String, String> configuration,
        final HiveConnectorInfoConverter infoConverter,
        final IMetacatHiveClient hiveMetastoreClient,
        final Registry registry
    ) {
        this.catalogName = catalogName;
        this.infoConverter = infoConverter;
        this.hiveMetastoreClient = hiveMetastoreClient;
        this.fastService = Boolean.parseBoolean(
            configuration.getOrDefault(HiveConfigConstants.USE_FASTHIVE_SERVICE, "false")
        );
        this.allowRenameTable = Boolean.parseBoolean(
            configuration.getOrDefault(HiveConfigConstants.ALLOW_RENAME_TABLE, "false")
        );
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure() {
        // TODO: Fix this properties binding so that it gets the proper metacat properties
        this.bind(Config.class).toInstance(new DefaultConfigImpl(new MetacatProperties()));
        this.bind(ThreadServiceManager.class).asEagerSingleton();
        this.bind(String.class).annotatedWith(Names.named("catalogName")).toInstance(catalogName);
        this.bind(Boolean.class).annotatedWith(Names.named("allowRenameTable")).toInstance(allowRenameTable);
        this.bind(HiveConnectorInfoConverter.class).toInstance(infoConverter);
        this.bind(Registry.class).toInstance(registry);
        this.bind(IMetacatHiveClient.class).toInstance(hiveMetastoreClient);
        this.bind(ConnectorDatabaseService.class).to(HiveConnectorDatabaseService.class).in(Scopes.SINGLETON);
        if (this.fastService) {
            this.bind(ConnectorPartitionService.class).to(HiveConnectorFastPartitionService.class).in(Scopes.SINGLETON);
            this.bind(ConnectorTableService.class).to(HiveConnectorFastTableService.class).in(Scopes.SINGLETON);
        } else {
            this.bind(ConnectorPartitionService.class).to(HiveConnectorPartitionService.class).in(Scopes.SINGLETON);
            this.bind(ConnectorTableService.class).to(HiveConnectorTableService.class).in(Scopes.SINGLETON);
        }
    }
}
