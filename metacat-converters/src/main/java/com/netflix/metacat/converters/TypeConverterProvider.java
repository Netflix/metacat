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

import static com.netflix.metacat.common.MetacatRequestContext.DataTypeContext.hive;
import static com.netflix.metacat.common.MetacatRequestContext.DataTypeContext.pig;
import static com.netflix.metacat.common.MetacatRequestContext.DataTypeContext.presto;

/**
 * @author amajumdar
 * @author tgianos
 */
public class TypeConverterProvider implements Provider<TypeConverter> {

    private final Config config;
    private final HiveTypeConverter hiveTypeConverter;
    private final PigTypeConverter pigTypeConverter;
    private final PrestoTypeConverter prestoTypeConverter;

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
        MetacatRequestContext requestContext = MetacatContextManager.getContext();
        MetacatRequestContext.DataTypeContext dataTypeContext = requestContext.getDataTypeContext();

        if (dataTypeContext == null) {
            return this.getDefaultConverter();
        } else {
            return this.get(dataTypeContext);
        }
    }

    public TypeConverter get(MetacatRequestContext.DataTypeContext context) {
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

    public MetacatRequestContext.DataTypeContext getDefaultConverterType() {
        TypeConverter converter = getDefaultConverter();
        if (converter instanceof HiveTypeConverter) {
            return hive;
        } else if (converter instanceof PigTypeConverter) {
            return pig;
        } else if (converter instanceof PrestoTypeConverter) {
            return presto;
        } else {
            throw new IllegalStateException("Unknown handler: " + converter);
        }
    }
}
