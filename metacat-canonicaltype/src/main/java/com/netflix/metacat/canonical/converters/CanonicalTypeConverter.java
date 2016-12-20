package com.netflix.metacat.canonical.converters;

import com.netflix.metacat.canonical.type.Type;
import com.netflix.metacat.canonical.type.TypeManager;

/**
 * Created by zhenli on 12/18/16.
 */
public interface CanonicalTypeConverter {
    /**
     * Converts to canonical type.
     * @param type type
     * @return canonical type
     */
    Type dataTypeToCanonicalType(String type, TypeManager typeRegistry);

    /**
     * Converts from canonical type.
     * @param type type
     * @return connector type
     */
    String canonicalTypeToDataType(Type type);
}
