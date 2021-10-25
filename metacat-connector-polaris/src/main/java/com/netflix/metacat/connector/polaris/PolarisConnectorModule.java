package com.netflix.metacat.connector.polaris;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.connectors.ConnectorUtils;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorDatabaseService;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorPartitionService;
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorTableService;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Module for the Polaris Connector.
 */
public class PolarisConnectorModule extends AbstractModule {
    private final String catalogShardName;
    private final Map<String, String> configuration;

    /**
     * Constructor.
     *
     * @param catalogShardName unique catalog shard name
     * @param configuration    connector configuration
     *
     */
    public PolarisConnectorModule(
        final String catalogShardName,
        final Map<String, String> configuration
    ) {
        this.catalogShardName = catalogShardName;
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        // TODO: bind polaris specific services as they become available
        this.bind(DataSource.class).toInstance(DataSourceManager.get()
            .load(this.catalogShardName, this.configuration).get(this.catalogShardName));
        this.bind(ConnectorDatabaseService.class)
            .to(ConnectorUtils.getDatabaseServiceClass(this.configuration, JdbcConnectorDatabaseService.class))
            .in(Scopes.SINGLETON);
        this.bind(ConnectorTableService.class)
            .to(ConnectorUtils.getTableServiceClass(this.configuration, JdbcConnectorTableService.class))
            .in(Scopes.SINGLETON);
        this.bind(ConnectorPartitionService.class)
            .to(ConnectorUtils.getPartitionServiceClass(this.configuration, JdbcConnectorPartitionService.class))
            .in(Scopes.SINGLETON);
    }
}
