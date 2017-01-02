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

package com.netflix.metacat.canonical.typeconverters;

import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.canonical.type.Base;
import com.netflix.metacat.canonical.type.BaseType;
import com.netflix.metacat.canonical.type.Type;
import lombok.Getter;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.pig.data.DataType;
import java.util.Map;


/**
 * Created by zhenli on 12/20/16.
 */

public final class TypeMapping {

    @Getter
    private static final Map<Type, String> CANONICAL_TO_HIVE = ImmutableMap.<Type, String>builder()
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
    private static final Map<String, Type> HIVE_TO_CANONICAL = ImmutableMap.<String, Type>builder()
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
    private static final Map<Type, Byte> CANONICAL_TO_PIG = new ImmutableMap.Builder<Type, Byte>()
        .put(BaseType.VARBINARY, Byte.valueOf(DataType.BYTEARRAY))
        .put(BaseType.BOOLEAN, Byte.valueOf(DataType.BOOLEAN))
        .put(BaseType.INT, Byte.valueOf(DataType.INTEGER))
        .put(BaseType.SMALLINT, Byte.valueOf(DataType.INTEGER))
        .put(BaseType.TINYINT, Byte.valueOf(DataType.INTEGER))
        .put(BaseType.BIGINT, Byte.valueOf(DataType.LONG))
        .put(BaseType.FLOAT, Byte.valueOf(DataType.FLOAT))
        .put(BaseType.DOUBLE, Byte.valueOf(DataType.DOUBLE))
        .put(BaseType.TIMESTAMP, Byte.valueOf(DataType.DATETIME))
        .put(BaseType.DATE, Byte.valueOf(DataType.DATETIME))
        .put(BaseType.STRING, Byte.valueOf(DataType.CHARARRAY))
        .put(BaseType.UNKNOWN, Byte.valueOf(DataType.UNKNOWN))
        .build();

    @Getter
    private static final Map<Byte, Type> PIG_TO_CANONICAL = new ImmutableMap.Builder<Byte, Type>()
        .put(Byte.valueOf(DataType.BOOLEAN), BaseType.BOOLEAN)
        .put(Byte.valueOf(DataType.UNKNOWN), BaseType.UNKNOWN)
        .put(Byte.valueOf(DataType.BYTE), BaseType.VARBINARY)
        .put(Byte.valueOf(DataType.BYTEARRAY), BaseType.VARBINARY)
        .put(Byte.valueOf(DataType.INTEGER), BaseType.INT)
        .put(Byte.valueOf(DataType.LONG), BaseType.BIGINT)
        .put(Byte.valueOf(DataType.BIGINTEGER), BaseType.BIGINT)
        .put(Byte.valueOf(DataType.FLOAT), BaseType.FLOAT)
        .put(Byte.valueOf(DataType.DOUBLE), BaseType.DOUBLE)
        .put(Byte.valueOf(DataType.BIGDECIMAL), BaseType.DOUBLE)
        .put(Byte.valueOf(DataType.DATETIME), BaseType.TIMESTAMP)
        .put(Byte.valueOf(DataType.CHARARRAY), BaseType.STRING)
        .put(Byte.valueOf(DataType.BIGCHARARRAY), BaseType.STRING)
        .build();

    private TypeMapping() {
    }
}
