package com.netflix.metacat.plugin.mysql;

import com.facebook.presto.plugin.jdbc.JdbcModule;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.netflix.metacat.common.util.DataSourceManager;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

public class MySqlJdbcConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module module;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;

    public MySqlJdbcConnectorFactory(String name, Module module, Map<String, String> optionalConfig, ClassLoader classLoader)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = checkNotNull(module, "module is null");
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig)
    {
        checkNotNull(requiredConfig, "requiredConfig is null");
        checkNotNull(optionalConfig, "optionalConfig is null");
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            DataSourceManager.get().load(connectorId, requiredConfig);
            Bootstrap app = new Bootstrap(new JdbcModule(connectorId), module);

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(MySqlJdbcConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
