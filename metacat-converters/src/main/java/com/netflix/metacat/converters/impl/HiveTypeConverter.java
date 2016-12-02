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

import com.facebook.presto.hive.util.Types;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.type.CharType;
import com.facebook.presto.type.DecimalType;
import com.facebook.presto.type.FloatType;
import com.facebook.presto.type.IntType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.SmallIntType;
import com.facebook.presto.type.StringType;
import com.facebook.presto.type.TinyIntType;
import com.facebook.presto.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.netflix.metacat.converters.TypeConverter;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Hive type converter.
 */
public class HiveTypeConverter implements TypeConverter {
    private static Type getPrimitiveType(final ObjectInspector fieldInspector) {
        final PrimitiveObjectInspector.PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) fieldInspector)
            .getPrimitiveCategory();
        switch (primitiveCategory) {
        case BOOLEAN:
            return BooleanType.BOOLEAN;
        case BYTE:
            return TinyIntType.TINY_INT;
        case SHORT:
            return SmallIntType.SMALL_INT;
        case INT:
            return IntType.INT;
        case LONG:
            return BigintType.BIGINT;
        case FLOAT:
            return FloatType.FLOAT;
        case DOUBLE:
            return DoubleType.DOUBLE;
        case DECIMAL:
            final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) ((PrimitiveObjectInspector) fieldInspector)
                .getTypeInfo();
            return DecimalType.createDecimalType(decimalTypeInfo.precision(), decimalTypeInfo.getScale());
        case CHAR:
            final int cLength = ((CharTypeInfo) ((PrimitiveObjectInspector) fieldInspector).getTypeInfo()).getLength();
            return CharType.createCharType(cLength);
        case STRING:
            return StringType.STRING;
        case VARCHAR:
            final int vLength = ((VarcharTypeInfo) ((PrimitiveObjectInspector) fieldInspector)
                .getTypeInfo()).getLength();
            return VarcharType.createVarcharType(vLength);
        case DATE:
            return DateType.DATE;
        case TIMESTAMP:
            return TimestampType.TIMESTAMP;
        case BINARY:
        case VOID:
            return VarbinaryType.VARBINARY;
        default:
            return null;
        }
    }

    /**
     * Returns the presto type.
     *
     * @param fieldInspector inspector
     * @param typeManager type manager
     * @return type
     */
    public Type getType(final ObjectInspector fieldInspector, final TypeManager typeManager) {
        switch (fieldInspector.getCategory()) {
        case PRIMITIVE:
            return getPrimitiveType(fieldInspector);
        case MAP:
            final MapObjectInspector mapObjectInspector = Types.checkType(fieldInspector, MapObjectInspector.class,
                "fieldInspector");
            final Type keyType = getType(mapObjectInspector.getMapKeyObjectInspector(), typeManager);
            final Type valueType = getType(mapObjectInspector.getMapValueObjectInspector(), typeManager);
            if (keyType == null || valueType == null) {
                return null;
            }
            return typeManager.getParameterizedType(StandardTypes.MAP,
                ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
        case LIST:
            final ListObjectInspector listObjectInspector = Types.checkType(fieldInspector, ListObjectInspector.class,
                "fieldInspector");
            final Type elementType = getType(listObjectInspector.getListElementObjectInspector(), typeManager);
            if (elementType == null) {
                return null;
            }
            return typeManager.getParameterizedType(StandardTypes.ARRAY,
                ImmutableList.of(elementType.getTypeSignature()), ImmutableList.of());
        case STRUCT:
            final StructObjectInspector structObjectInspector =
                Types.checkType(fieldInspector, StructObjectInspector.class, "fieldInspector");
            final List<TypeSignature> fieldTypes = new ArrayList<>();
            final List<Object> fieldNames = new ArrayList<>();
            for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                fieldNames.add(field.getFieldName());
                final Type fieldType = getType(field.getFieldObjectInspector(), typeManager);
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
    public String fromType(final Type type) {
        if (BooleanType.BOOLEAN.equals(type)) {
            return serdeConstants.BOOLEAN_TYPE_NAME;
        } else if (TinyIntType.TINY_INT.equals(type)) {
            return serdeConstants.TINYINT_TYPE_NAME;
        } else if (SmallIntType.SMALL_INT.equals(type)) {
            return serdeConstants.SMALLINT_TYPE_NAME;
        } else if (IntType.INT.equals(type)) {
            return serdeConstants.INT_TYPE_NAME;
        } else if (BigintType.BIGINT.equals(type)) {
            return serdeConstants.BIGINT_TYPE_NAME;
        } else if (FloatType.FLOAT.equals(type)) {
            return serdeConstants.FLOAT_TYPE_NAME;
        } else if (DoubleType.DOUBLE.equals(type)) {
            return serdeConstants.DOUBLE_TYPE_NAME;
        } else if (type instanceof DecimalType) {
            return type.getDisplayName();
        } else if (type instanceof CharType) {
            return type.getDisplayName();
        } else if (StringType.STRING.equals(type)) {
            return serdeConstants.STRING_TYPE_NAME;
        } else if (type instanceof VarcharType) {
            return type.getDisplayName();
        } else if (VarbinaryType.VARBINARY.equals(type)) {
            return serdeConstants.BINARY_TYPE_NAME;
        } else if (DateType.DATE.equals(type)) {
            return serdeConstants.DATE_TYPE_NAME;
        } else if (TimestampType.TIMESTAMP.equals(type)) {
            return serdeConstants.TIMESTAMP_TYPE_NAME;
        } else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
            final MapType mapType = (MapType) type;
            return "map<" + fromType(mapType.getKeyType()) + "," + fromType(mapType.getValueType()) + ">";
        } else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
            final String typeString = type.getTypeParameters().stream().map(this::fromType)
                .collect(Collectors.joining(","));
            return "array<" + typeString + ">";
        } else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW)) {
            final RowType rowType = (RowType) type;
            final String typeString = rowType.getFields()
                .stream()
                .map(this::rowFieldToString)
                .collect(Collectors.joining(","));
            return "struct<" + typeString + ">";
        } else {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "unsupported type: " + type);
        }
    }

    private String rowFieldToString(final RowType.RowField rowField) {
        String prefix = "";
        if (rowField.getName().isPresent()) {
            prefix = rowField.getName().get() + ":";
        }

        return prefix + fromType(rowField.getType());
    }

    @Override
    public Type toType(final String type, final TypeManager typeManager) {
        // Hack to fix presto "varchar" type coming in with no length which is required by Hive.
        final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
            "varchar".equals(type) ? serdeConstants.STRING_TYPE_NAME : type);
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
        // The standard struct object inspector forces field names to lower case, however in Metacat we need to preserve
        // the original case of the struct fields so we wrap it with our wrapper to force the fieldNames to keep
        // their original case
        if (typeInfo.getCategory().equals(ObjectInspector.Category.STRUCT)) {
            final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
            final StandardStructObjectInspector objectInspector = (StandardStructObjectInspector) oi;
            oi = new SameCaseStandardStructObjectInspector(structTypeInfo.getAllStructFieldNames(), objectInspector);
        }
        return getType(oi, typeManager);
    }

    // This is protected and extends StandardStructObjectInspector so it can reference MyField
    protected static class SameCaseStandardStructObjectInspector extends StandardStructObjectInspector {
        private final List<String> realFieldNames;
        private final StandardStructObjectInspector structObjectInspector;

        public SameCaseStandardStructObjectInspector(final List<String> realFieldNames,
            final StandardStructObjectInspector structObjectInspector) {
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
            public SameCaseMyField(final int fieldID, final String fieldName,
                final ObjectInspector fieldObjectInspector,
                final String fieldComment) {
                super(fieldID, fieldName, fieldObjectInspector, fieldComment);
                // Since super lower cases fieldName, this is to restore the original case
                this.fieldName = fieldName;
            }
        }
    }
}
