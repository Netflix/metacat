package com.netflix.metacat.plugin.postgresql;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.postgresql.PostgreSqlClientModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class MetacatPostgreSqlClientModule extends PostgreSqlClientModule {
    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).to(MetacatPostgreSqlClient.class).in(Scopes.SINGLETON);
        binder.bind(PostgreSqlJdbcConnector.class).in(Scopes.SINGLETON);
        binder.bind(PostgreSqlJdbcMetadata.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
    }
}
