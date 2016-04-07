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

package com.netflix.metacat.converters.impl;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.netflix.metacat.converters.TypeConverter;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.FloatType.FLOAT;
import static com.facebook.presto.type.IntType.INT;
import static com.facebook.presto.type.TinyIntType.TINY_INT;
import static com.facebook.presto.type.SmallIntType.SMALL_INT;
import static com.facebook.presto.type.DecimalType.DECIMAL;
import static com.facebook.presto.type.CharType.CHAR;
import static com.facebook.presto.type.StringType.STRING;
import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.CHAR_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TIMESTAMP_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.VARCHAR_TYPE_NAME;

public class HiveTypeConverter implements TypeConverter {
    private static Type getPrimitiveType(PrimitiveObjectInspector.PrimitiveCategory primitiveCategory) {
        switch (primitiveCategory) {
        case BOOLEAN:
            return BOOLEAN;
        case BYTE:
            return TINY_INT;
        case SHORT:
            return SMALL_INT;
        case INT:
            return INT;
        case LONG:
            return BIGINT;
        case FLOAT:
            return FLOAT;
        case DOUBLE:
            return DOUBLE;
        case DECIMAL:
            return DECIMAL;
        case CHAR:
            return CHAR;
        case STRING:
            return STRING;
        case VARCHAR:
            return VARCHAR;
        case DATE:
            return DATE;
        case TIMESTAMP:
            return TIMESTAMP;
        case BINARY:
        case VOID:
            return VARBINARY;
        default:
            return null;
        }
    }

    public static Type getType(ObjectInspector fieldInspector, TypeManager typeManager) {
        switch (fieldInspector.getCategory()) {
        case PRIMITIVE:
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) fieldInspector).getPrimitiveCategory();
            return getPrimitiveType(primitiveCategory);
        case MAP:
            MapObjectInspector mapObjectInspector = checkType(fieldInspector, MapObjectInspector.class,
                    "fieldInspector");
            Type keyType = getType(mapObjectInspector.getMapKeyObjectInspector(), typeManager);
            Type valueType = getType(mapObjectInspector.getMapValueObjectInspector(), typeManager);
            if (keyType == null || valueType == null) {
                return null;
            }
            return typeManager.getParameterizedType(StandardTypes.MAP,
                    ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
        case LIST:
            ListObjectInspector listObjectInspector = checkType(fieldInspector, ListObjectInspector.class,
                    "fieldInspector");
            Type elementType = getType(listObjectInspector.getListElementObjectInspector(), typeManager);
            if (elementType == null) {
                return null;
            }
            return typeManager.getParameterizedType(StandardTypes.ARRAY,
                    ImmutableList.of(elementType.getTypeSignature()), ImmutableList.of());
        case STRUCT:
            StructObjectInspector structObjectInspector = checkType(fieldInspector, StructObjectInspector.class,
                    "fieldInspector");
            List<TypeSignature> fieldTypes = new ArrayList<>();
            List<Object> fieldNames = new ArrayList<>();
            for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                fieldNames.add(field.getFieldName());
                Type fieldType = getType(field.getFieldObjectInspector(), typeManager);
                if (fieldType == null) {
                    return null;
                }
                fieldTypes.add(fieldType.getTypeSignature());
            }
            return typeManager.getParameterizedType(StandardTypes.ROW, fieldTypes, fieldNames);
        default:
            throw new IllegalArgumentException("Unsupported hive type " + fieldInspector.getTypeName());
        }
    }

    @Override
    public String fromType(Type type) {
        if (BOOLEAN.equals(type)) {
            return BOOLEAN_TYPE_NAME;
        } else if (TINY_INT.equals(type)) {
            return TINYINT_TYPE_NAME;
        } else if (SMALL_INT.equals(type)) {
            return SMALLINT_TYPE_NAME;
        } else if (INT.equals(type)) {
            return INT_TYPE_NAME;
        } else if (BIGINT.equals(type)) {
            return BIGINT_TYPE_NAME;
        } else if (FLOAT.equals(type)) {
            return FLOAT_TYPE_NAME;
        } else if (DOUBLE.equals(type)) {
            return DOUBLE_TYPE_NAME;
        } else if (DECIMAL.equals(type)) {
            return DECIMAL_TYPE_NAME;
        } else if (CHAR.equals(type)) {
            return CHAR_TYPE_NAME;
        } else if (STRING.equals(type)) {
            return STRING_TYPE_NAME;
        } else if (VARCHAR.equals(type)) {
            return VARCHAR_TYPE_NAME;
        } else if (VARBINARY.equals(type)) {
            return BINARY_TYPE_NAME;
        } else if (DateType.DATE.equals(type)) {
            return DATE_TYPE_NAME;
        } else if (TIMESTAMP.equals(type)) {
            return TIMESTAMP_TYPE_NAME;
        } else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
            MapType mapType = (MapType) type;
            return "map<" + fromType(mapType.getKeyType()) + "," + fromType(mapType.getValueType()) + ">";
        } else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
            String typeString = type.getTypeParameters().stream().map(this::fromType).collect(Collectors.joining(","));
            return "array<" + typeString + ">";
        } else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW)) {
            RowType rowType = (RowType) type;
            String typeString = rowType.getFields()
                    .stream()
                    .map(this::rowFieldToString)
                    .collect(Collectors.joining(","));
            return "struct<" + typeString + ">";
        } else {
            throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
        }
    }

    private String rowFieldToString(RowType.RowField rowField) {
        String prefix = "";
        if (rowField.getName().isPresent()) {
            prefix = rowField.getName().get() + ":";
        }

        return prefix + fromType(rowField.getType());
    }

    @Override
    public Type toType(String type, TypeManager typeManager) {
        // Hack to fix presto "varchar" type coming in with no length which is required by Hive.
        if ("varchar".equals(type)) {
            type = STRING_TYPE_NAME;
        }
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
        // The standard struct object inspector forces field names to lower case, however in Metacat we need to preserve
        // the original case of the struct fields so we wrap it with our wrapper to force the fieldNames to keep
        // their original case
        if (typeInfo.getCategory().equals(ObjectInspector.Category.STRUCT)) {
            StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
            StandardStructObjectInspector objectInspector = (StandardStructObjectInspector) oi;
            oi = new SameCaseStandardStructObjectInspector(structTypeInfo.getAllStructFieldNames(), objectInspector);
        }
        return getType(oi, typeManager);
    }

    // This is protected and extends StandardStructObjectInspector so it can reference MyField
    protected static class SameCaseStandardStructObjectInspector extends StandardStructObjectInspector {
        private final List<String> realFieldNames;
        private final StandardStructObjectInspector structObjectInspector;

        public SameCaseStandardStructObjectInspector(List<String> realFieldNames,
                StandardStructObjectInspector structObjectInspector) {
            this.realFieldNames = realFieldNames;
            this.structObjectInspector = structObjectInspector;
        }

        @Override
        public List<? extends StructField> getAllStructFieldRefs() {
            return structObjectInspector.getAllStructFieldRefs()
                    .stream()
                    .map(structField -> (MyField) structField)
                    .map(field -> new SameCaseMyField(field.getFieldID(), realFieldNames.get(field.getFieldID()),
                            field.getFieldObjectInspector(), field.getFieldComment()))
                    .collect(Collectors.toList());
        }

        protected static class SameCaseMyField extends MyField {
            public SameCaseMyField(int fieldID, String fieldName, ObjectInspector fieldObjectInspector,
                    String fieldComment) {
                super(fieldID, fieldName, fieldObjectInspector, fieldComment);
                // Since super lower cases fieldName, this is to restore the original case
                this.fieldName = fieldName;
            }
        }
    }
}
