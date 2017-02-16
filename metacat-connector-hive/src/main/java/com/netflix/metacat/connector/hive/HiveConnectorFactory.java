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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.persist.PersistService;
import com.google.inject.persist.jpa.JpaPersistModule;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.util.DataSourceManager;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;

import java.util.Map;

/**
 * HiveConnectorFactory.
 * @author zhenl
 */
public class HiveConnectorFactory implements ConnectorFactory {
    private final String name;
    private final Map<String, String> configuration;
    private final HiveConnectorInfoConverter infoConverter;
    private ConnectorDatabaseService databaseService;
    private ConnectorTableService tableService;
    private ConnectorPartitionService partitionService;
    private PersistService persistService;


    /**
     * Constructor.
     * @param name connector name. Also the catalog name.
     * @param configuration configuration properties
     * @param infoConverter S3 info converter
     */
    public HiveConnectorFactory(final String name, final Map<String, String> configuration,
                              final HiveConnectorInfoConverter infoConverter) {
        Preconditions.checkNotNull(name, "Catalog name is null");
        Preconditions.checkNotNull(configuration, "Catalog connector configuration is null");
        this.name = name;
        this.configuration = configuration;
        this.infoConverter = infoConverter;
        init();
    }

    private void init() {
        //JPA module
        final Map<String, Object> props = Maps.newHashMap(configuration);
        props.put("hibernate.connection.datasource",
            DataSourceManager.get().load(name, configuration).get(name));
        final Module jpaModule = new JpaPersistModule("s3").properties(props);
        final Module s3Module = new HiveConnectorModule(name, configuration, infoConverter);
        final Injector injector = Guice.createInjector(jpaModule, s3Module);
        persistService = injector.getInstance(PersistService.class);
        persistService.start();
        this.databaseService = injector.getInstance(ConnectorDatabaseService.class);
        this.tableService = injector.getInstance(ConnectorTableService.class);
        this.partitionService = injector.getInstance(ConnectorPartitionService.class);
    }

    @Override
    public ConnectorDatabaseService getDatabaseService() {
        return databaseService;
    }

    @Override
    public ConnectorTableService getTableService() {
        return tableService;
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        return partitionService;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void stop() {
        persistService.stop();
    }
}
