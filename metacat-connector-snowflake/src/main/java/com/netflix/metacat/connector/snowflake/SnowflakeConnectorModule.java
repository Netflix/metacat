/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package com.netflix.metacat.connector.snowflake;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper;
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorDatabaseService;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorPartitionService;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Guice module for the Snowflake Connector.
 *
 * @author amajumdar
 * @since 1.2.0
 */
public class SnowflakeConnectorModule extends AbstractModule {
    private static final String DATABASE_KEY = "metacat.snowflake.database";
    private final String catalogShardName;
    private final Map<String, String> configuration;
    private final String database;

    /**
     * Constructor.
     *
     * @param catalogShardName unique catalog shard name
     * @param configuration    connector configuration
     *
     */
    public SnowflakeConnectorModule(
        final String catalogShardName,
        final Map<String, String> configuration
    ) {
        this.catalogShardName = catalogShardName;
        this.configuration = configuration;
        this.database = configuration.getOrDefault(DATABASE_KEY, "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        this.bind(String.class).annotatedWith(Names.named("database")).toInstance(this.database);
        this.bind(DataSource.class).toInstance(DataSourceManager.get()
            .load(this.catalogShardName, this.configuration).get(this.catalogShardName));
        this.bind(JdbcTypeConverter.class).to(SnowflakeTypeConverter.class).in(Scopes.SINGLETON);
        this.bind(JdbcExceptionMapper.class).to(SnowflakeExceptionMapper.class).in(Scopes.SINGLETON);
        this.bind(ConnectorDatabaseService.class)
            .to(ConnectorUtils.getDatabaseServiceClass(this.configuration, JdbcConnectorDatabaseService.class))
            .in(Scopes.SINGLETON);
        this.bind(ConnectorTableService.class)
            .to(ConnectorUtils.getTableServiceClass(this.configuration, SnowflakeConnectorTableService.class))
            .in(Scopes.SINGLETON);
        this.bind(ConnectorPartitionService.class)
            .to(ConnectorUtils.getPartitionServiceClass(this.configuration, JdbcConnectorPartitionService.class))
            .in(Scopes.SINGLETON);
    }
}
