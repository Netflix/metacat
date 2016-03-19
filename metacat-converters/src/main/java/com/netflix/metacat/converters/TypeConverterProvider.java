package com.netflix.metacat.converters;

import com.google.common.base.Throwables;
import com.netflix.metacat.common.MetacatContext;
import com.netflix.metacat.common.server.Config;
import com.netflix.metacat.common.util.MetacatContextManager;
import com.netflix.metacat.converters.impl.HiveTypeConverter;
import com.netflix.metacat.converters.impl.PigTypeConverter;
import com.netflix.metacat.converters.impl.PrestoTypeConverter;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.netflix.metacat.common.MetacatContext.DATA_TYPE_CONTEXTS.hive;
import static com.netflix.metacat.common.MetacatContext.DATA_TYPE_CONTEXTS.pig;
import static com.netflix.metacat.common.MetacatContext.DATA_TYPE_CONTEXTS.presto;

/**
 * Created by amajumdar on 10/7/15.
 */
public class TypeConverterProvider implements Provider<TypeConverter> {
    @Inject
    Config config;
    @Inject
    HiveTypeConverter hiveTypeConverter;
    @Inject
    PigTypeConverter pigTypeConverter;
    @Inject
    PrestoTypeConverter prestoTypeConverter;

    @Override
    public TypeConverter get() {
        MetacatContext metacatContext = MetacatContextManager.getContext();
        String dataTypeContext = metacatContext.getDataTypeContext();
        if (hive.name().equalsIgnoreCase(dataTypeContext)) {
            return hiveTypeConverter;
        } else if (pig.name().equalsIgnoreCase(dataTypeContext)) {
            return pigTypeConverter;
        } else if (presto.name().equalsIgnoreCase(dataTypeContext)) {
            return prestoTypeConverter;
        } else {
            return getDefaultConverter();
        }
    }

    public TypeConverter get(MetacatContext.DATA_TYPE_CONTEXTS context) {
        switch (context) {
        case hive:
            return hiveTypeConverter;
        case pig:
            return pigTypeConverter;
        case presto:
            return prestoTypeConverter;
        default:
            throw new IllegalArgumentException("No handler for " + context);
        }
    }

    public TypeConverter getDefaultConverter() {
        try {
            return (TypeConverter) Class.forName(config.getDefaultTypeConverter()).newInstance();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public MetacatContext.DATA_TYPE_CONTEXTS getDefaultConverterType() {
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
