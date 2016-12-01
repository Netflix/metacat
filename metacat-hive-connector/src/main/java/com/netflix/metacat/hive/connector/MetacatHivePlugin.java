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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.metacat.hive.connector;

import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.CharType;
import com.facebook.presto.type.DecimalType;
import com.facebook.presto.type.FloatType;
import com.facebook.presto.type.IntType;
import com.facebook.presto.type.SmallIntType;
import com.facebook.presto.type.StringType;
import com.facebook.presto.type.TinyIntType;
import com.facebook.presto.type.VarcharType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * Metacat hive plugin.
 */
public class MetacatHivePlugin implements Plugin {
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private TypeManager typeManager;

    @Inject
    public void setTypeManager(final TypeManager typeManager) {
        this.typeManager = Preconditions.checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public void setOptionalConfig(final Map<String, String> optionalConfig) {
        this.optionalConfig = ImmutableMap.copyOf(Preconditions.checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public <T> List<T> getServices(final Class<T> type) {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type
                .cast(new MetacatHiveConnectorFactory("metacat-hive", optionalConfig, getClassLoader(), typeManager)));
        } else if (type == Type.class) {
            return ImmutableList
                .of(type.cast(FloatType.FLOAT), type.cast(IntType.INT), type.cast(TinyIntType.TINY_INT),
                    type.cast(SmallIntType.SMALL_INT), type.cast(DecimalType.DECIMAL),
                    type.cast(CharType.CHAR), type.cast(VarcharType.VARCHAR), type.cast(StringType.STRING));
        }
        return ImmutableList.of();
    }

    private static ClassLoader getClassLoader() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = MetacatHivePlugin.class.getClassLoader();
        }
        return classLoader;
    }
}
