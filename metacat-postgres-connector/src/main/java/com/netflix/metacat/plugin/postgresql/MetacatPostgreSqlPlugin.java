package com.netflix.metacat.plugin.postgresql;

import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.type.FloatType.FLOAT;
import static com.facebook.presto.type.IntType.INT;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;

public class MetacatPostgreSqlPlugin implements Plugin {
    private static final String NAME = "metacat-postgresql";
    private static final Module MODULE = new MetacatPostgreSqlClientModule();
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new PostgreSqlJdbcConnectorFactory(NAME, MODULE, optionalConfig, getClassLoader())));
        } else if (type == Type.class){
            return ImmutableList.of(type.cast(FLOAT), type.cast(INT));
        }
        return ImmutableList.of();
    }

    private static ClassLoader getClassLoader()
    {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), MetacatPostgreSqlPlugin.class.getClassLoader());
    }
}
