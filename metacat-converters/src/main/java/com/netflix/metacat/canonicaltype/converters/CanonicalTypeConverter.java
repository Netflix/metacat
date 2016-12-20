package com.netflix.metacat.canonicaltype.converters;

import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeRegistry;

/**
 * Created by zhenli on 12/18/16.
 */
public interface CanonicalTypeConverter {
    /**
     * Converts to canonical type.
     * @param type type
     * @return canonical type
     */
    Type dataTypeToCanonicalType(String type, TypeRegistry typeRegistry);

    /**
     * Converts from canonical type.
     * @param type type
     * @return connector type
     */
    String canonicalTypeToDataType(Type type);
}
