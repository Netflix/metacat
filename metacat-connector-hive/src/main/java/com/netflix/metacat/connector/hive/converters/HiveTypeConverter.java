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
import com.google.common.collect.Lists;
import com.netflix.iceberg.PartitionField;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.types.Types;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.MapType;
import com.netflix.metacat.common.type.ParametricType;
import com.netflix.metacat.common.type.RowType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeEnum;
import com.netflix.metacat.common.type.TypeRegistry;
import com.netflix.metacat.common.type.TypeSignature;
import com.netflix.metacat.common.type.TypeUtils;
import com.netflix.metacat.common.type.VarcharType;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
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
 * Class to convert hive to canonical type and vice versa.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public class HiveTypeConverter implements ConnectorTypeConverter {

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

    @Override
    public Type toMetacatType(final String type) {
        // Hack to fix presto "varchar" type coming in with no length which is required by Hive.
        final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
            "varchar".equals(type.toLowerCase()) ? serdeConstants.STRING_TYPE_NAME : type);
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
        return getCanonicalType(oi);
    }

    /**
     * Converts iceberg schema to field dto.
     *
     * @param schema          schema
     * @param partitionFields partitioned fields
     * @return list of field Info
     */
    public List<FieldInfo> icebergeSchemaTofieldDtos(final Schema schema,
                                                     final List<PartitionField> partitionFields) {
        final List<FieldInfo> fields = Lists.newArrayList();
        final List<String> partitionNames =
            partitionFields.stream()
                .map(f -> schema.findField(f.sourceId()).name()).collect(Collectors.toList());

        for (Types.NestedField field : schema.columns()) {
            final FieldInfo fieldInfo = new FieldInfo();
            fieldInfo.setName(field.name());
            fieldInfo.setType(toMetacatType(fromIcebergToHiveType(field.type())));
            fieldInfo.setIsNullable(field.isOptional());
            fieldInfo.setComment(field.doc());
            fieldInfo.setPartitionKey(partitionNames.contains(field.name()));
            fields.add(fieldInfo);
        }

        return fields;
    }



    /**
     * convert iceberg to hive type.
     * @param type iceberg type.
     * @return hive type string.
     */
    public static String fromIcebergToHiveType(final com.netflix.iceberg.types.Type type) {
        switch (type.typeId()) {
            case BOOLEAN:
                return serdeConstants.BOOLEAN_TYPE_NAME;
            case INTEGER:
                return serdeConstants.INT_TYPE_NAME;
            case LONG:
                return serdeConstants.BIGINT_TYPE_NAME;
            case FLOAT:
                return serdeConstants.FLOAT_TYPE_NAME;
            case DOUBLE:
                return serdeConstants.DOUBLE_TYPE_NAME;
            case DATE:
                return serdeConstants.DATE_TYPE_NAME;
            case TIME:
                throw new UnsupportedOperationException("Hive does not support time fields");
            case TIMESTAMP:
                return serdeConstants.TIMESTAMP_TYPE_NAME;
            case STRING:
            case UUID:
                return serdeConstants.STRING_TYPE_NAME;
            case FIXED:
                return serdeConstants.BINARY_TYPE_NAME;
            case BINARY:
                return serdeConstants.BINARY_TYPE_NAME;
            case DECIMAL:
                final Types.DecimalType decimalType = (Types.DecimalType) type;
                return String.format("decimal(%s,%s)", decimalType.precision(), decimalType.scale());
            case STRUCT:
                final Types.StructType structType = type.asStructType();
                final String nameToType = (String) structType.fields().stream().map((f) -> {
                    return String.format("%s:%s", f.name(), fromIcebergToHiveType(f.type()));
                }).collect(Collectors.joining(","));
                return String.format("struct<%s>", nameToType);
            case LIST:
                final Types.ListType listType = type.asListType();
                return String.format("array<%s>", fromIcebergToHiveType(listType.elementType()));
            case MAP:
                final Types.MapType mapType = type.asMapType();
                return String.format("map<%s,%s>", fromIcebergToHiveType(mapType.keyType()),
                    fromIcebergToHiveType(mapType.valueType()));
            default:
                throw new UnsupportedOperationException(type + " is not supported");
        }
    }

    @Override
    public String fromMetacatType(final Type type) {
        if (HiveTypeMapping.getCANONICAL_TO_HIVE().containsKey(type)) {
            return HiveTypeMapping.getCANONICAL_TO_HIVE().get(type);
        }
        if (type instanceof DecimalType | type instanceof CharType | type instanceof VarcharType) {
            return type.getDisplayName();
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.MAP)) {
            final MapType mapType = (MapType) type;
            return "map<" + fromMetacatType(mapType.getKeyType())
                + "," + fromMetacatType(mapType.getValueType()) + ">";
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.ROW)) {
            final RowType rowType = (RowType) type;
            final String typeString = rowType.getFields()
                .stream()
                .map(this::rowFieldToString)
                .collect(Collectors.joining(","));
            return "struct<" + typeString + ">";
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.ARRAY)) {
            final String typeString = ((ParametricType) type).getParameters().stream().map(this::fromMetacatType)
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

    /**
     * Returns the canonical type.
     *
     * @param fieldInspector inspector
     * @return type
     */
    Type getCanonicalType(final ObjectInspector fieldInspector) {
        switch (fieldInspector.getCategory()) {
            case PRIMITIVE:
                return getPrimitiveType(fieldInspector);
            case MAP:
                final MapObjectInspector mapObjectInspector =
                    TypeUtils.checkType(fieldInspector, MapObjectInspector.class,
                        "fieldInspector");
                final Type keyType = getCanonicalType(mapObjectInspector.getMapKeyObjectInspector());
                final Type valueType = getCanonicalType(mapObjectInspector.getMapValueObjectInspector());
                if (keyType == null || valueType == null) {
                    return null;
                }
                return TypeRegistry.getTypeRegistry().getParameterizedType(TypeEnum.MAP,
                    ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
            case LIST:
                final ListObjectInspector listObjectInspector =
                    TypeUtils.checkType(fieldInspector, ListObjectInspector.class,
                        "fieldInspector");
                final Type elementType =
                    getCanonicalType(listObjectInspector.getListElementObjectInspector());
                if (elementType == null) {
                    return null;
                }
                return TypeRegistry.getTypeRegistry().getParameterizedType(TypeEnum.ARRAY,
                    ImmutableList.of(elementType.getTypeSignature()), ImmutableList.of());
            case STRUCT:
                final StructObjectInspector structObjectInspector =
                    TypeUtils.checkType(fieldInspector, StructObjectInspector.class, "fieldInspector");
                final List<TypeSignature> fieldTypes = new ArrayList<>();
                final List<Object> fieldNames = new ArrayList<>();
                for (StructField field : structObjectInspector.getAllStructFieldRefs()) {
                    fieldNames.add(field.getFieldName());
                    final Type fieldType = getCanonicalType(field.getFieldObjectInspector());
                    if (fieldType == null) {
                        return null;
                    }
                    fieldTypes.add(fieldType.getTypeSignature());
                }
                return TypeRegistry.getTypeRegistry()
                    .getParameterizedType(TypeEnum.ROW, fieldTypes, fieldNames);
            default:
                log.info("Currently unsupported type {}, returning Unknown type", fieldInspector.getTypeName());
                return BaseType.UNKNOWN;
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
