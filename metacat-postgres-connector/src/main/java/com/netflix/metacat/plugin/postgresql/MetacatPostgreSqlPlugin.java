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
