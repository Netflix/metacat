/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.metacat.connector.pig.converters;

import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.VarbinaryType;
import lombok.Getter;
import org.apache.pig.data.DataType;

import java.util.Map;

/**
 * Pig type mapping.
 */
public class PigTypeMapping {
    @Getter
    private static final Map<Type, Byte> CANONICAL_TO_PIG = new ImmutableMap.Builder<Type, Byte>()
        .put(VarbinaryType.VARBINARY, Byte.valueOf(DataType.BYTEARRAY))
        .put(BaseType.BOOLEAN, Byte.valueOf(DataType.BOOLEAN))
        .put(BaseType.INT, Byte.valueOf(DataType.INTEGER))
        .put(BaseType.SMALLINT, Byte.valueOf(DataType.INTEGER))
        .put(BaseType.TINYINT, Byte.valueOf(DataType.INTEGER))
        .put(BaseType.BIGINT, Byte.valueOf(DataType.LONG))
        .put(BaseType.FLOAT, Byte.valueOf(DataType.FLOAT))
        .put(BaseType.DOUBLE, Byte.valueOf(DataType.DOUBLE))
        .put(BaseType.TIMESTAMP, Byte.valueOf(DataType.DATETIME))
        .put(BaseType.TIMESTAMP_WITH_TIME_ZONE, Byte.valueOf(DataType.DATETIME))
        .put(BaseType.DATE, Byte.valueOf(DataType.DATETIME))
        .put(BaseType.TIME, Byte.valueOf(DataType.DATETIME))
        .put(BaseType.STRING, Byte.valueOf(DataType.CHARARRAY))
        .put(BaseType.UNKNOWN, Byte.valueOf(DataType.UNKNOWN))
        .build();

    @Getter
    private static final Map<Byte, Type> PIG_TO_CANONICAL = new ImmutableMap.Builder<Byte, Type>()
        .put(Byte.valueOf(DataType.BOOLEAN), BaseType.BOOLEAN)
        .put(Byte.valueOf(DataType.UNKNOWN), BaseType.UNKNOWN)
        .put(Byte.valueOf(DataType.BYTE), VarbinaryType.VARBINARY)
        .put(Byte.valueOf(DataType.BYTEARRAY), VarbinaryType.VARBINARY)
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
}
