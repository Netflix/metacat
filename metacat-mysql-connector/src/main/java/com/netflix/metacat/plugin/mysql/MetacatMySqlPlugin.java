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

import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.FloatType;
import com.facebook.presto.type.IntType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;

import java.util.List;
import java.util.Map;

/**
 * Mysql plugin.
 */
public class MetacatMySqlPlugin implements Plugin {
    private static final String NAME = "metacat-mysql";
    private static final Module MODULE = new MetacatMySqlClientModule();
    private Map<String, String> optionalConfig = ImmutableMap.of();

    @Override
    public void setOptionalConfig(final Map<String, String> optionalConfig) {
        this.optionalConfig = ImmutableMap.copyOf(Preconditions.checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public <T> List<T> getServices(final Class<T> type) {
        if (type == ConnectorFactory.class) {
            return ImmutableList
                .of(type.cast(new MySqlJdbcConnectorFactory(NAME, MODULE, optionalConfig, getClassLoader())));
        } else if (type == Type.class) {
            return ImmutableList.of(type.cast(FloatType.FLOAT), type.cast(IntType.INT));
        }
        return ImmutableList.of();
    }

    private static ClassLoader getClassLoader() {
        return MoreObjects.firstNonNull(Thread.currentThread().getContextClassLoader(),
            MetacatMySqlPlugin.class.getClassLoader());
    }
}
