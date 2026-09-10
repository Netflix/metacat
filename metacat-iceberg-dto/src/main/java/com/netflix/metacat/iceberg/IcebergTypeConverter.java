/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeEnum;
import com.netflix.metacat.common.type.TypeFormatter;
import com.netflix.metacat.common.type.TypeRegistry;
import com.netflix.metacat.common.type.TypeSignature;
import com.netflix.metacat.common.type.VarbinaryType;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

// Converts Iceberg types to Metacat canonical types and on to the forms carried on a field DTO.
// Mapping directly avoids the Hive parser the connector round trips through.
final class IcebergTypeConverter {

    private IcebergTypeConverter() {
    }

    static Type toCanonicalType(final org.apache.iceberg.types.Type type) {
        switch (type.typeId()) {
            case BOOLEAN:
                return BaseType.BOOLEAN;
            case INTEGER:
                return BaseType.INT;
            case LONG:
                return BaseType.BIGINT;
            case FLOAT:
                return BaseType.FLOAT;
            case DOUBLE:
                return BaseType.DOUBLE;
            case DATE:
                return BaseType.DATE;
            case TIME:
                throw new UnsupportedOperationException("Hive does not support time fields");
            case TIMESTAMP:
                return BaseType.TIMESTAMP;
            case STRING:
            case UUID:
                return BaseType.STRING;
            case FIXED:
            case BINARY:
                return VarbinaryType.VARBINARY;
            case DECIMAL:
                final Types.DecimalType decimal = (Types.DecimalType) type;
                return DecimalType.createDecimalType(decimal.precision(), decimal.scale());
            case STRUCT:
                return toRowType(type.asStructType());
            case LIST:
                return TypeRegistry.getTypeRegistry().getParameterizedType(TypeEnum.ARRAY,
                    ImmutableList.of(toCanonicalType(type.asListType().elementType()).getTypeSignature()),
                    ImmutableList.of());
            case MAP:
                final Types.MapType map = type.asMapType();
                return TypeRegistry.getTypeRegistry().getParameterizedType(TypeEnum.MAP,
                    ImmutableList.of(toCanonicalType(map.keyType()).getTypeSignature(),
                        toCanonicalType(map.valueType()).getTypeSignature()),
                    ImmutableList.of());
            default:
                throw new UnsupportedOperationException(type + " is not supported");
        }
    }

    static String toTypeString(final org.apache.iceberg.types.Type type) {
        return TypeFormatter.toHiveString(toCanonicalType(type));
    }

    static JsonNode toJsonType(final org.apache.iceberg.types.Type type) {
        return TypeFormatter.toJson(toCanonicalType(type), TypeFormatter::toHiveString);
    }

    private static Type toRowType(final Types.StructType struct) {
        final List<TypeSignature> fieldTypes = new ArrayList<>(struct.fields().size());
        final List<Object> fieldNames = new ArrayList<>(struct.fields().size());
        for (final Types.NestedField field : struct.fields()) {
            fieldNames.add(field.name());
            fieldTypes.add(toCanonicalType(field.type()).getTypeSignature());
        }
        return TypeRegistry.getTypeRegistry().getParameterizedType(TypeEnum.ROW, fieldTypes, fieldNames);
    }
}
