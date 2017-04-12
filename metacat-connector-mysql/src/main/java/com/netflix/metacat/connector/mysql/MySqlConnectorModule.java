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
package com.netflix.metacat.connector.mysql;

import com.google.inject.Scopes;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorModule;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorPartitionService;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorTableService;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.util.Map;

/**
 * A Guice Module for the MySqlConnector.
 *
 * @author tgianos
 * @since 1.0.0
 */
public class MySqlConnectorModule extends ConnectorModule {

    private final String name;
    private final Map<String, String> configuration;

    /**
     * Constructor.
     *
     * @param name          The name of the catalog this module is associated will be associated with
     * @param configuration The connector configuration
     */
    MySqlConnectorModule(
        @Nonnull @NonNull final String name,
        @Nonnull @NonNull final Map<String, String> configuration
    ) {
        this.name = name;
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        this.bind(DataSource.class)
            .toInstance(DataSourceManager.get().load(this.name, this.configuration).get(this.name));
        this.bind(JdbcTypeConverter.class).to(MySqlTypeConverter.class).in(Scopes.SINGLETON);
        this.bind(JdbcExceptionMapper.class).to(MySqlExceptionMapper.class).in(Scopes.SINGLETON);
        this.bind(ConnectorDatabaseService.class)
            .to(this.getDatabaseServiceClass(this.configuration, MySqlConnectorDatabaseService.class))
            .in(Scopes.SINGLETON);
        this.bind(ConnectorTableService.class)
            .to(this.getTableServiceClass(this.configuration, JdbcConnectorTableService.class))
            .in(Scopes.SINGLETON);
        this.bind(ConnectorPartitionService.class)
            .to(this.getPartitionServiceClass(this.configuration, JdbcConnectorPartitionService.class))
            .in(Scopes.SINGLETON);
    }
}
