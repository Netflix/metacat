package com.netflix.metacat.canonical.converters;

import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.canonical.type.Base;
import com.netflix.metacat.canonical.type.BaseType;
import com.netflix.metacat.canonical.type.Type;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import java.util.Map;

/**
 * Created by zhenli on 12/20/16.
 */

public final class TypeMapping {

    private static final Map<Type, String> CANONICALTOHIVE = ImmutableMap.<Type, String>builder()
        .put(BaseType.TINYINT, serdeConstants.TINYINT_TYPE_NAME)
        .put(BaseType.SMALLINT, serdeConstants.SMALLINT_TYPE_NAME)
        .put(BaseType.INT, serdeConstants.INT_TYPE_NAME)
        .put(BaseType.BIGINT, serdeConstants.BIGINT_TYPE_NAME)
        .put(BaseType.FLOAT, serdeConstants.FLOAT_TYPE_NAME)
        .put(BaseType.DOUBLE, serdeConstants.DOUBLE_TYPE_NAME)
        .put(BaseType.BOOLEAN, serdeConstants.BOOLEAN_TYPE_NAME)
        .put(BaseType.STRING, serdeConstants.STRING_TYPE_NAME)
        .put(BaseType.VARBINARY, serdeConstants.BINARY_TYPE_NAME)
        .put(BaseType.DATE, serdeConstants.DATE_TYPE_NAME)
        .put(BaseType.TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME)
        .build();

    private static final Map<String, Type> HIVETOCANONICAL = ImmutableMap.<String, Type>builder()
        .put(PrimitiveCategory.BOOLEAN.name(), BaseType.BOOLEAN)
        .put(PrimitiveCategory.BYTE.name(), BaseType.TINYINT)
        .put(PrimitiveCategory.SHORT.name(), BaseType.SMALLINT)
        .put(PrimitiveCategory.INT.name(), BaseType.INT)
        .put(PrimitiveCategory.LONG.name(), BaseType.BIGINT)
        .put(PrimitiveCategory.FLOAT.name(), BaseType.FLOAT)
        .put(PrimitiveCategory.DOUBLE.name(), BaseType.DOUBLE)
        .put(PrimitiveCategory.DATE.name(), BaseType.DATE)
        .put(PrimitiveCategory.TIMESTAMP.name(), BaseType.TIMESTAMP)
        .put(PrimitiveCategory.BINARY.name(), BaseType.VARBINARY)
        .put(PrimitiveCategory.VOID.name(), BaseType.VARBINARY)
        .put(PrimitiveCategory.STRING.name(), BaseType.STRING)

//        .put(Base.DECIMAL, BaseType.DECIMAL)
//        .put(Base.CHAR, BaseType.CHAR)
//        .put(Base.VARCHAR, BaseType.VARCHAR)

        .put(Base.DATE.getBaseTypeDisplayName(), BaseType.DATE)

        .build();

    private TypeMapping() {
    }

    /**
     * Get canonical type to hive type mapping.
     *
     * @return mapping from canonical type to hive type
     */
    public static Map<Type, String> getCanonicalToHiveType() {
        return CANONICALTOHIVE;
    }

    /**
     * Get hive type from to canonical type mapping.
     *
     * @return mapping from hive to canonical type
     */
    public static Map<String, Type> getHiveToCanonical() {
        return HIVETOCANONICAL;
    }
}
