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

package com.netflix.metacat.plugin.mysql;

import com.facebook.presto.plugin.jdbc.JdbcModule;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.netflix.metacat.common.util.DataSourceManager;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

/**
 * Mysql connector factory.
 */
public class MySqlJdbcConnectorFactory
    implements ConnectorFactory {
    private final String name;
    private final Module module;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;

    /**
     * Constructor.
     * @param name name
     * @param module guice module
     * @param optionalConfig config
     * @param classLoader class loader
     */
    public MySqlJdbcConnectorFactory(final String name, final Module module, final Map<String, String> optionalConfig,
        final ClassLoader classLoader) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = Preconditions.checkNotNull(module, "module is null");
        this.optionalConfig = ImmutableMap.copyOf(Preconditions.checkNotNull(optionalConfig, "optionalConfig is null"));
        this.classLoader = Preconditions.checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Connector create(final String connectorId, final Map<String, String> requiredConfig) {
        Preconditions.checkNotNull(requiredConfig, "requiredConfig is null");
        Preconditions.checkNotNull(optionalConfig, "optionalConfig is null");
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            DataSourceManager.get().load(connectorId, requiredConfig);
            final Bootstrap app = new Bootstrap(new JdbcModule(connectorId), module);

            final Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .setOptionalConfigurationProperties(optionalConfig)
                .initialize();

            return injector.getInstance(MySqlJdbcConnector.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
