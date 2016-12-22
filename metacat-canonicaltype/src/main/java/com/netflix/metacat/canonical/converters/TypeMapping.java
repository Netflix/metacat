package com.netflix.metacat.canonical.converters;

import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.canonical.type.Base;
import com.netflix.metacat.canonical.type.BaseType;
import com.netflix.metacat.canonical.type.Type;
import lombok.Getter;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.pig.data.DataType;

import java.util.Map;

import static java.lang.Byte.valueOf;

/**
 * Created by zhenli on 12/20/16.
 */

public final class TypeMapping {

    @Getter
    private static final Map<Type, String> CanonicalToHive = ImmutableMap.<Type, String>builder()
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

    @Getter
    private static final Map<String, Type> HiveToCanonical = ImmutableMap.<String, Type>builder()
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
        .put(Base.DATE.getBaseTypeDisplayName(), BaseType.DATE)
        .build();

    @Getter
    private static final Map<Type, Byte> CanonicalToPig = new ImmutableMap.Builder<Type, Byte>()
        .put(BaseType.VARBINARY, valueOf(DataType.BYTEARRAY))
        .put(BaseType.BOOLEAN, valueOf(DataType.BOOLEAN))
        .put(BaseType.INT, valueOf(DataType.INTEGER))
        .put(BaseType.SMALLINT, valueOf(DataType.INTEGER))
        .put(BaseType.TINYINT, valueOf(DataType.INTEGER))
        .put(BaseType.BIGINT, valueOf(DataType.LONG))
        .put(BaseType.FLOAT, valueOf(DataType.FLOAT))
        .put(BaseType.DOUBLE, valueOf(DataType.DOUBLE))
        .put(BaseType.TIMESTAMP, valueOf(DataType.DATETIME))
        .put(BaseType.DATE, valueOf(DataType.DATETIME))
        .put(BaseType.STRING, valueOf(DataType.CHARARRAY))
        .put(BaseType.UNKNOWN, valueOf(DataType.UNKNOWN))
        .build();

    @Getter
    private static final Map<Byte, Type> PigToCanonical = new ImmutableMap.Builder<Byte, Type>()
        .put(valueOf(DataType.BOOLEAN), BaseType.BOOLEAN)
        .put(valueOf(DataType.UNKNOWN), BaseType.UNKNOWN)
        .put(valueOf(DataType.BYTE), BaseType.VARBINARY)
        .put(valueOf(DataType.BYTEARRAY), BaseType.VARBINARY)
        .put(valueOf(DataType.INTEGER), BaseType.INT)
        .put(valueOf(DataType.LONG), BaseType.BIGINT)
        .put(valueOf(DataType.BIGINTEGER), BaseType.BIGINT)
        .put(valueOf(DataType.FLOAT), BaseType.FLOAT)
        .put(valueOf(DataType.DOUBLE), BaseType.DOUBLE)
        .put(valueOf(DataType.BIGDECIMAL), BaseType.DOUBLE)
        .put(valueOf(DataType.DATETIME), BaseType.TIMESTAMP)
        .put(valueOf(DataType.CHARARRAY), BaseType.STRING)
        .put(valueOf(DataType.BIGCHARARRAY), BaseType.STRING)
        .build();

    private TypeMapping() {
    }
}
