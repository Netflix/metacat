/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.plugin.postgresql;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.postgresql.PostgreSqlClientModule;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigBinder;

/**
 * Guice module.
 */
public class MetacatPostgreSqlClientModule extends PostgreSqlClientModule {
    @Override
    public void configure(final Binder binder) {
        binder.bind(JdbcClient.class).to(MetacatPostgreSqlClient.class).in(Scopes.SINGLETON);
        binder.bind(PostgreSqlJdbcConnector.class).in(Scopes.SINGLETON);
        binder.bind(PostgreSqlJdbcMetadata.class).in(Scopes.SINGLETON);
        ConfigBinder.configBinder(binder).bindConfig(BaseJdbcConfig.class);
    }
}
