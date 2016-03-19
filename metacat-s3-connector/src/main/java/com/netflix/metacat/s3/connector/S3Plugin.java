package com.netflix.metacat.s3.connector;

import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.type.FloatType.FLOAT;
import static com.facebook.presto.type.IntType.INT;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by amajumdar on 10/9/15.
 */
public class S3Plugin implements Plugin
{
    private TypeManager typeManager;
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Override
    public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public synchronized void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    public synchronized Map<String, String> getOptionalConfig()
    {
        return optionalConfig;
    }

    @Override
    public synchronized <T> List<T> getServices(Class<T> type)
    {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(
                    new S3ConnectorFactory(typeManager, getOptionalConfig(), getClassLoader())));
        } else if (type == Type.class){
            return ImmutableList.of(type.cast(FLOAT), type.cast(INT));
        }
        return ImmutableList.of();
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = S3Plugin.class.getClassLoader();
        }
        return classLoader;
    }
}
