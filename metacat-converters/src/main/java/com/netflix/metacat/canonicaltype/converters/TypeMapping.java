package com.netflix.metacat.canonicaltype.converters;

import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.common.type.Base;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.Type;
import org.apache.hadoop.hive.serde.serdeConstants;

import java.util.Map;

/**
 * Created by zhenli on 12/20/16.
 */
public class TypeMapping {
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
        .put(Base.BOOLEAN.getBaseTypeDisplayName(), BaseType.BOOLEAN)
        .put(Base.TINYINT.getBaseTypeDisplayName(), BaseType.TINYINT)
        .put(Base.SMALLINT.getBaseTypeDisplayName(), BaseType.SMALLINT)
        .put(Base.INT.getBaseTypeDisplayName(), BaseType.INT)
        .put(Base.BIGINT.getBaseTypeDisplayName(), BaseType.BIGINT)
        .put(Base.FLOAT.getBaseTypeDisplayName(), BaseType.FLOAT)
        .put(Base.DOUBLE.getBaseTypeDisplayName(), BaseType.DOUBLE)
//        .put(Base.DECIMAL, BaseType.DECIMAL)
//        .put(Base.CHAR, BaseType.CHAR)
//        .put(Base.VARCHAR, BaseType.VARCHAR)
        .put(Base.STRING.getBaseTypeDisplayName(), BaseType.STRING)
        .put("binary", BaseType.VARBINARY)
        .put(Base.DATE.getBaseTypeDisplayName(), BaseType.DATE)
        .put(Base.TIMESTAMP.getBaseTypeDisplayName(), BaseType.TIMESTAMP)
        .build();

    /**
     * Get canonical type to hive type mapping.
     * @return mapping from canonical type to hive type
     */
    public static Map<Type, String> getCanonicalToHiveType() {
        return CANONICALTOHIVE;
    }

    /**
     * Get hive type from to canonical type mapping.
     * @return mapping from hive to canonical type
     */
    public static Map<String, Type> getHiveToCanonical() {
        return HIVETOCANONICAL;
    }
}
