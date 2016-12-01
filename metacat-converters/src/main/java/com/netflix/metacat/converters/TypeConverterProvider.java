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

package com.netflix.metacat.converters;

import com.google.common.base.Throwables;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.converters.impl.HiveTypeConverter;
import com.netflix.metacat.converters.impl.PigTypeConverter;
import com.netflix.metacat.converters.impl.PrestoTypeConverter;

import javax.inject.Inject;
import javax.inject.Provider;

/**
 * Type converter provider.
 * @author amajumdar
 * @author tgianos
 */
public class TypeConverterProvider implements Provider<TypeConverter> {

    private final Config config;
    private final HiveTypeConverter hiveTypeConverter;
    private final PigTypeConverter pigTypeConverter;
    private final PrestoTypeConverter prestoTypeConverter;

    /**
     * Constructor.
     * @param config config
     * @param hiveTypeConverter hive type converter
     * @param pigTypeConverter pig type converter
     * @param prestoTypeConverter presto type converter
     */
    @Inject
    public TypeConverterProvider(
        final Config config,
        final HiveTypeConverter hiveTypeConverter,
        final PigTypeConverter pigTypeConverter,
        final PrestoTypeConverter prestoTypeConverter
    ) {
        this.config = config;
        this.hiveTypeConverter = hiveTypeConverter;
        this.pigTypeConverter = pigTypeConverter;
        this.prestoTypeConverter = prestoTypeConverter;
    }

    @Override
    public TypeConverter get() {
        final MetacatRequestContext requestContext = MetacatContextManager.getContext();
        final MetacatRequestContext.DataTypeContext dataTypeContext = requestContext.getDataTypeContext();

        if (dataTypeContext == null) {
            return this.getDefaultConverter();
        } else {
            return this.get(dataTypeContext);
        }
    }

    /**
     * Returns the right type converter based on the context.
     * @param context context
     * @return type converter
     */
    public TypeConverter get(final MetacatRequestContext.DataTypeContext context) {
        switch (context) {
        case hive:
            return this.hiveTypeConverter;
        case pig:
            return this.pigTypeConverter;
        case presto:
            return this.prestoTypeConverter;
        default:
            throw new IllegalArgumentException("No handler for " + context);
        }
    }

    private TypeConverter getDefaultConverter() {
        try {
            return (TypeConverter) Class.forName(config.getDefaultTypeConverter()).newInstance();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Returns the default converter.
     * @return converter
     */
    public MetacatRequestContext.DataTypeContext getDefaultConverterType() {
        final TypeConverter converter = getDefaultConverter();
        if (converter instanceof HiveTypeConverter) {
            return MetacatRequestContext.DataTypeContext.hive;
        } else if (converter instanceof PigTypeConverter) {
            return MetacatRequestContext.DataTypeContext.pig;
        } else if (converter instanceof PrestoTypeConverter) {
            return MetacatRequestContext.DataTypeContext.presto;
        } else {
            throw new IllegalStateException("Unknown handler: " + converter);
        }
    }
}
