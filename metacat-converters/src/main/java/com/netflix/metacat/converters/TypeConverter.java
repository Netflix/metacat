package com.netflix.metacat.converters;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

/**
 * Created by amajumdar on 4/28/15.
 */
public interface TypeConverter {
    Type toType(String type, TypeManager typeManager);
    String fromType(Type type);
}
