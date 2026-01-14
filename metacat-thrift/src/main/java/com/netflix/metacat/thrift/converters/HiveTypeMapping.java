/*
 *  Copyright 2016 Netflix, Inc.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.thrift.converters;

import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeEnum;
import com.netflix.metacat.common.type.VarbinaryType;
import lombok.Getter;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.Map;

/**
 * Hive type mapping.
 *
 * @author zhenl
 * @since 1.0.0
 */
public class HiveTypeMapping {

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
        .put(VarbinaryType.VARBINARY, serdeConstants.BINARY_TYPE_NAME)
        .put(BaseType.DATE, serdeConstants.DATE_TYPE_NAME)
        .put(BaseType.TIMESTAMP, serdeConstants.TIMESTAMP_TYPE_NAME)
        .build();

    @Getter
    private static final Map<String, Type> HIVE_TO_CANONICAL = ImmutableMap.<String, Type>builder()
        .put(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN.name(), BaseType.BOOLEAN)
        .put(PrimitiveObjectInspector.PrimitiveCategory.BYTE.name(), BaseType.TINYINT)
        .put(PrimitiveObjectInspector.PrimitiveCategory.SHORT.name(), BaseType.SMALLINT)
        .put(PrimitiveObjectInspector.PrimitiveCategory.INT.name(), BaseType.INT)
        .put(PrimitiveObjectInspector.PrimitiveCategory.LONG.name(), BaseType.BIGINT)
        .put(PrimitiveObjectInspector.PrimitiveCategory.FLOAT.name(), BaseType.FLOAT)
        .put(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE.name(), BaseType.DOUBLE)
        .put(PrimitiveObjectInspector.PrimitiveCategory.DATE.name(), BaseType.DATE)
        .put(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP.name(), BaseType.TIMESTAMP)
        .put(PrimitiveObjectInspector.PrimitiveCategory.BINARY.name(), VarbinaryType.VARBINARY)
        .put(PrimitiveObjectInspector.PrimitiveCategory.VOID.name(), VarbinaryType.VARBINARY)
        .put(PrimitiveObjectInspector.PrimitiveCategory.STRING.name(), BaseType.STRING)
        .put(TypeEnum.DATE.getType(), BaseType.DATE)
        .build();
}
