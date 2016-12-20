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

package com.netflix.metacat.canonical.converters;
import com.google.common.collect.ImmutableList;
import com.netflix.metacat.canonical.type.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class to convert hive to canonical type and vice versa.
 */
public class CanonicalHiveTypeConverter implements CanonicalTypeConverter {

    @Override
    public Type dataTypeToCanonicalType(final String type, final TypeManager typeRegistry) {
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
        return getCanonicalType(oi, typeRegistry);
    }

    @Override
    public String canonicalTypeToDataType(final Type type) {
        if (TypeMapping.getCanonicalToHiveType().containsKey(type)) {
            return TypeMapping.getCanonicalToHiveType().get(type);
        }
        if (type instanceof DecimalType) {
            return ((DecimalType) type).getDisplayName();
        }
        else if (type instanceof CharType) {
            return ((CharType) type).getDisplayName();
        }
        return null;
    }

    private static Type getPrimitiveType(final ObjectInspector fieldInspector) {
        final PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) fieldInspector)
            .getPrimitiveCategory();
        switch (primitiveCategory) {
            case BOOLEAN:
                return BaseType.BOOLEAN;
            case BYTE:
                return BaseType.TINYINT;
            case SHORT:
                return BaseType.SMALLINT;
            case INT:
                return BaseType.INT;
            case LONG:
                return BaseType.BIGINT;
            case FLOAT:
                return BaseType.FLOAT;
            case DOUBLE:
                return BaseType.DOUBLE;
            case DATE:
                return BaseType.DATE;
            case TIMESTAMP:
                return BaseType.TIMESTAMP;
            case BINARY:
            case VOID://not sure?
                return BaseType.VARBINARY;
            case DECIMAL:
                final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) ((PrimitiveObjectInspector) fieldInspector)
                    .getTypeInfo();
                return DecimalType.createDecimalType(decimalTypeInfo.precision(), decimalTypeInfo.getScale());
            case CHAR:
                final int cLength = ((CharTypeInfo) ((PrimitiveObjectInspector) fieldInspector).getTypeInfo()).getLength();
                return CharType.createCharType(cLength);
        }
        return null;
    }

    /**
     * Returns the canonical type.
     *
     * @param fieldInspector inspector
     * @param typeRegistry type manager
     * @return type
     */
    public Type getCanonicalType(final ObjectInspector fieldInspector, final TypeManager typeRegistry) {
        switch (fieldInspector.getCategory()) {
            case PRIMITIVE:
                return getPrimitiveType(fieldInspector);
            case MAP:
                final MapObjectInspector mapObjectInspector = TypeUtil.checkType(fieldInspector, MapObjectInspector.class,
                    "fieldInspector");
                final Type keyType = getCanonicalType(mapObjectInspector.getMapKeyObjectInspector(), typeRegistry);
                final Type valueType = getCanonicalType(mapObjectInspector.getMapValueObjectInspector(), typeRegistry);
                if (keyType == null || valueType == null) {
                    return null;
                }
                return typeRegistry.getParameterizedType(Base.MAP.getBaseTypeDisplayName(),
                    ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
            case LIST:
                final ListObjectInspector listObjectInspector = TypeUtil.checkType(fieldInspector, ListObjectInspector.class,
                    "fieldInspector");
                final Type elementType = getCanonicalType(listObjectInspector.getListElementObjectInspector(), typeRegistry);
                if (elementType == null) {
                    return null;
                }
                return typeRegistry.getParameterizedType(Base.ARRAY.getBaseTypeDisplayName(),
                    ImmutableList.of(elementType.getTypeSignature()), ImmutableList.of());
            case STRUCT:
                final StructObjectInspector structObjectInspector =
                    TypeUtil.checkType(fieldInspector, StructObjectInspector.class, "fieldInspector");
                final List<TypeSignature> fieldTypes = new ArrayList<>();
                final List<Object> fieldNames = new ArrayList<>();
                for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                    fieldNames.add(field.getFieldName());
                    final Type fieldType = getCanonicalType(field.getFieldObjectInspector(), typeRegistry);
                    if (fieldType == null) {
                        return null;
                    }
                    fieldTypes.add(fieldType.getTypeSignature());
                }
                return typeRegistry.getParameterizedType(Base.ROW.getBaseTypeDisplayName(), fieldTypes, fieldNames);
            default:
                throw new IllegalArgumentException("Unsupported hive type " + fieldInspector.getTypeName());
        }
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
                .map(field -> new SameCaseStandardStructObjectInspector.SameCaseMyField(field.getFieldID(),
                    realFieldNames.get(field.getFieldID()),
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
