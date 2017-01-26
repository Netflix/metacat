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

package com.netflix.metacat.connector.hive.converters;

import com.google.common.collect.ImmutableList;
import com.netflix.metacat.common.type.TypeConverter;
import com.netflix.metacat.common.type.TypeEnum;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.MapType;
import com.netflix.metacat.common.type.RowType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeManager;
import com.netflix.metacat.common.type.TypeSignature;
import com.netflix.metacat.common.type.TypeUtils;
import com.netflix.metacat.common.type.VarcharType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class to convert hive to canonical type and vice versa.
 * @author zhenl
 */
public class HiveTypeConverter implements TypeConverter {

    @Override
    public Type toMetacatType(final String type, final TypeManager typeRegistry) {
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
            oi = new HiveTypeConverter.SameCaseStandardStructObjectInspector(
                structTypeInfo.getAllStructFieldNames(), objectInspector);
        }
        return getCanonicalType(oi, typeRegistry);
    }

    @Override
    public String fromMetacatType(final Type type) {
        if (HiveTypeMapping.getCANONICAL_TO_HIVE().containsKey(type)) {
            return HiveTypeMapping.getCANONICAL_TO_HIVE().get(type);
        }
        if (type instanceof DecimalType) {
            return ((DecimalType) type).getDisplayName();
        } else if (type instanceof CharType) {
            return ((CharType) type).getDisplayName();
        } else if (type instanceof VarcharType) {
            return ((VarcharType) type).getDisplayName();
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.MAP.getBaseTypeDisplayName())) {
            final MapType mapType = (MapType) type;
            return "map<" + fromMetacatType(mapType.getKeyType())
                + "," + fromMetacatType(mapType.getValueType()) + ">";
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.ROW.getBaseTypeDisplayName())) {
            final RowType rowType = (RowType) type;
            final String typeString = rowType.getFields()
                .stream()
                .map(this::rowFieldToString)
                .collect(Collectors.joining(","));
            return "struct<" + typeString + ">";
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.ARRAY.getBaseTypeDisplayName())) {
            final String typeString = type.getParameters().stream().map(this::fromMetacatType)
                .collect(Collectors.joining(","));
            return "array<" + typeString + ">";
        }
        return null;
    }

    private String rowFieldToString(final RowType.RowField rowField) {
        String prefix = "";
        if (rowField.getName() != null) {
            prefix = rowField.getName() + ":";
        }
        return prefix + fromMetacatType(rowField.getType());
    }

    private static Type getPrimitiveType(final ObjectInspector fieldInspector) {
        final PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) fieldInspector)
            .getPrimitiveCategory();
        if (HiveTypeMapping.getHIVE_TO_CANONICAL().containsKey(primitiveCategory.name())) {
            return HiveTypeMapping.getHIVE_TO_CANONICAL().get(primitiveCategory.name());
        }
        switch (primitiveCategory) {
            case DECIMAL:
                final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) ((PrimitiveObjectInspector) fieldInspector)
                    .getTypeInfo();
                return DecimalType.createDecimalType(decimalTypeInfo.precision(), decimalTypeInfo.getScale());
            case CHAR:
                final int cLength = ((CharTypeInfo) ((PrimitiveObjectInspector)
                    fieldInspector).getTypeInfo()).getLength();
                return CharType.createCharType(cLength);
            case VARCHAR:
                final int vLength = ((VarcharTypeInfo) ((PrimitiveObjectInspector) fieldInspector)
                    .getTypeInfo()).getLength();
                return VarcharType.createVarcharType(vLength);
            default:
                return null;
        }
    }

    /**
     * Returns the canonical type.
     *
     * @param fieldInspector inspector
     * @param typeRegistry   type manager
     * @return type
     */
    Type getCanonicalType(final ObjectInspector fieldInspector, final TypeManager typeRegistry) {
        switch (fieldInspector.getCategory()) {
            case PRIMITIVE:
                return getPrimitiveType(fieldInspector);
            case MAP:
                final MapObjectInspector mapObjectInspector =
                    TypeUtils.checkType(fieldInspector, MapObjectInspector.class,
                        "fieldInspector");
                final Type keyType = getCanonicalType(mapObjectInspector.getMapKeyObjectInspector(), typeRegistry);
                final Type valueType = getCanonicalType(mapObjectInspector.getMapValueObjectInspector(), typeRegistry);
                if (keyType == null || valueType == null) {
                    return null;
                }
                return typeRegistry.getParameterizedType(TypeEnum.MAP.getBaseTypeDisplayName(),
                    ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
            case LIST:
                final ListObjectInspector listObjectInspector =
                    TypeUtils.checkType(fieldInspector, ListObjectInspector.class,
                        "fieldInspector");
                final Type elementType =
                    getCanonicalType(listObjectInspector.getListElementObjectInspector(), typeRegistry);
                if (elementType == null) {
                    return null;
                }
                return typeRegistry.getParameterizedType(TypeEnum.ARRAY.getBaseTypeDisplayName(),
                    ImmutableList.of(elementType.getTypeSignature()), ImmutableList.of());
            case STRUCT:
                final StructObjectInspector structObjectInspector =
                    TypeUtils.checkType(fieldInspector, StructObjectInspector.class, "fieldInspector");
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
                return typeRegistry.getParameterizedType(TypeEnum.ROW.getBaseTypeDisplayName(), fieldTypes, fieldNames);
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
                .map(field -> new HiveTypeConverter.
                    SameCaseStandardStructObjectInspector.SameCaseMyField(field.getFieldID(),
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
