package com.netflix.metacat.plugin.mysql;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.mysql.MySqlClientModule;
import com.facebook.presto.plugin.mysql.MySqlConfig;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class MetacatMySqlClientModule extends MySqlClientModule {
    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).to(MetacatMySqlClient.class).in(Scopes.SINGLETON);
        binder.bind(MySqlJdbcConnector.class).in(Scopes.SINGLETON);
        binder.bind(MySqlJdbcMetadata.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(MySqlConfig.class);
    }
}
