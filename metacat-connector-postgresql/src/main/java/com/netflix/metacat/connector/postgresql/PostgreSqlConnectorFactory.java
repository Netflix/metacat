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
package com.netflix.metacat.connector.postgresql;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * PostgreSQL implementation of a connector factory.
 *
 * @author tgianos
 * @since 0.1.52
 */
@Slf4j
public class PostgreSqlConnectorFactory implements ConnectorFactory {

    private final String name;
    private final Injector injector;

    /**
     * Constructor.
     *
     * @param name          The catalog name
     * @param configuration The catalog configuration
     */
    PostgreSqlConnectorFactory(
        @Nonnull @NonNull final String name,
        @Nonnull @NonNull final Map<String, String> configuration
    ) {
        log.info("Creating connector factory for catalog {}", name);
        this.name = name;
        this.injector = Guice.createInjector(new PostgreSqlConnectorModule(name, configuration));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorDatabaseService getDatabaseService() {
        return this.injector.getInstance(ConnectorDatabaseService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTableService getTableService() {
        return this.injector.getInstance(ConnectorTableService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorPartitionService getPartitionService() {
        return this.injector.getInstance(ConnectorPartitionService.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        // The data source is closed by DataSourceManager @PreDestroy method
    }
}
