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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Class to convert hive to canonical type and vice versa.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public class HiveTypeConverter implements ConnectorTypeConverter {

    // matches decimal declarations with only scale, ex: decimal(38)
    // matches declarations with spaces around '(', the scale and ')'
    private static final String DECIMAL_WITH_SCALE
        = "decimal\\s*\\(\\s*[0-9]+\\s*\\)";

    // matches decimal declarations with scale and precision, ex: decimal(38,9)
    // matches declarations with spaces around '(', the scale, the precision, the comma and ')'
    private static final String DECIMAL_WITH_SCALE_AND_PRECISION
        = "decimal\\s*\\(\\s*[0-9]+\\s*,\\s*[0-9]*\\s*\\)";

    // combined compiled pattern to match both
    private static final Pattern DECIMAL_TYPE
        = Pattern.compile(DECIMAL_WITH_SCALE + "|" + DECIMAL_WITH_SCALE_AND_PRECISION, Pattern.CASE_INSENSITIVE);

    private static Type getPrimitiveType(final TypeInfo typeInfo) {
        final PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
        if (HiveTypeMapping.getHIVE_TO_CANONICAL().containsKey(primitiveCategory.name())) {
            return HiveTypeMapping.getHIVE_TO_CANONICAL().get(primitiveCategory.name());
        }
        switch (primitiveCategory) {
            case DECIMAL:
                final DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                return DecimalType.createDecimalType(decimalTypeInfo.precision(), decimalTypeInfo.getScale());
            case CHAR:
                final int cLength = ((CharTypeInfo) typeInfo).getLength();
                return CharType.createCharType(cLength);
            case VARCHAR:
                final int vLength = ((VarcharTypeInfo) typeInfo).getLength();
                return VarcharType.createVarcharType(vLength);
            default:
                return null;
        }
    }

    @Override
    public Type toMetacatType(final String type) {
        // Hack to fix presto "varchar" type coming in with no length which is required by Hive.
        final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(sanitizeType(type));
        return getCanonicalType(typeInfo);
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
            final org.apache.iceberg.types.Type fieldType = field.type();
            fieldInfo.setSourceType(fieldType.toString());
            fieldInfo.setType(toMetacatType(fromIcebergToHiveType(fieldType)));
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
    public static String fromIcebergToHiveType(final org.apache.iceberg.types.Type type) {
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
     * Sanitize the type to handle Hive type conversion edge cases.
     *
     * @param type the type to sanitize
     * @return the sanitized type
     */
    public static String sanitizeType(final String type) {
        if ("varchar".equalsIgnoreCase(type)) {
            return serdeConstants.STRING_TYPE_NAME;
        } else {
            // the current version of Hive (1.2.1) cannot handle spaces in column definitions
            // this was fixed in 1.3.0. See: https://issues.apache.org/jira/browse/HIVE-11476
            // this bug caused an error in loading the table information in Metacat
            // see: https://netflix.slack.com/archives/G0SUNC804/p1676930065306799
            // Here the offending column definition was decimal(38, 9)
            // which had a space between the command and the digit 9

            // instead of upgrading the Hive version, we are making a targeted "fix"
            // to handle this space in a decimal column declaration

            // the regex we use tries to match various decimal declarations
            // and handles decimal types inside other type declarations like array and struct
            // see the unit test for those method for all the cases handled
            final Matcher matcher = DECIMAL_TYPE.matcher(type);
            final StringBuilder replacedType = new StringBuilder();

            // keep track of the start of the substring that we haven't matched yet
            // more explanation on how this is used is below
            int prevStart = 0;

            // we cannot simply use matcher.matches() and matcher.replaceAll()
            // because that will replace the decimal declaration itself
            // instead we use the region APIs (start() and end()) to find the substring that matched
            // and then apply the replace function to remove spaces in the decimal declaration
            // we do this for all the matches in the type declaration and hence the usage of the while loop
            while (matcher.find()) {
                // this index represents the start index (inclusive) of our current match
                final int currMatchStart = matcher.start();

                // this represents the end index (exclusive) of our current match
                final int currMatchEnd = matcher.end();

                replacedType
                    // first append any part of the string that did not match
                    // this is represented by the prevStart (inclusive) till the start of the current match (exclusive)
                    // this append should not need any replacement and can be added verbatim
                    .append(type, prevStart, currMatchStart)

                    // Then append the matching part which should be a decimal declaration
                    // The matching part is start (inclusive) and end (exclusive)
                    // This part should go through a replacement to remove spaces
                    .append(type.substring(currMatchStart, currMatchEnd).replaceAll("\\s", ""));

                // update the prevStart marker so that for the next match
                // we know where to start to add the non-matching part
                prevStart = currMatchEnd;
            }

            // append any remaining part of the input type to the final answer
            // again, no replacement necessary for this part since it should not contain and decimal declarations
            // phew!
            replacedType.append(type.substring(prevStart));

            return replacedType.toString();
        }
    }

    /**
     * Returns the canonical type.
     *
     * @param typeInfo typeInfo
     * @return Metacat Type
     */
    Type getCanonicalType(final TypeInfo typeInfo) {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                return getPrimitiveType(typeInfo);
            case MAP:
                final MapTypeInfo mapTypeInfo =
                    TypeUtils.checkType(typeInfo, MapTypeInfo.class, "typeInfo");
                final Type keyType = getCanonicalType(mapTypeInfo.getMapKeyTypeInfo());
                final Type valueType = getCanonicalType(mapTypeInfo.getMapValueTypeInfo());
                if (keyType == null || valueType == null) {
                    return null;
                }
                return TypeRegistry.getTypeRegistry().getParameterizedType(TypeEnum.MAP,
                    ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()), ImmutableList.of());
            case LIST:
                final ListTypeInfo listTypeInfo =
                    TypeUtils.checkType(typeInfo, ListTypeInfo.class, "typeInfo");
                final Type elementType =
                    getCanonicalType(listTypeInfo.getListElementTypeInfo());
                if (elementType == null) {
                    return null;
                }
                return TypeRegistry.getTypeRegistry().getParameterizedType(TypeEnum.ARRAY,
                    ImmutableList.of(elementType.getTypeSignature()), ImmutableList.of());
            case STRUCT:
                final StructTypeInfo structTypeInfo =
                    TypeUtils.checkType(typeInfo, StructTypeInfo.class, "typeInfo");
                // Hive struct type infos
                final List<String> structFieldNames = structTypeInfo.getAllStructFieldNames();
                final List<TypeInfo> structFieldTypeInfos = structTypeInfo.getAllStructFieldTypeInfos();
                final int structInfoCounts = structFieldNames.size();

                // Metacat canonical type infos
                final List<TypeSignature> fieldTypes = new ArrayList<>(structInfoCounts);
                final List<Object> fieldNames = new ArrayList<>(structInfoCounts);

                for (int i = 0; i < structInfoCounts; i++) {
                    fieldNames.add(structFieldNames.get(i));
                    final Type fieldType = getCanonicalType(structFieldTypeInfos.get(i));
                    if (fieldType == null) {
                        return null;
                    }
                    fieldTypes.add(fieldType.getTypeSignature());
                }
                return TypeRegistry.getTypeRegistry()
                    .getParameterizedType(TypeEnum.ROW, fieldTypes, fieldNames);
            default:
                log.info("Currently unsupported type {}, returning Unknown type", typeInfo.getTypeName());
                return BaseType.UNKNOWN;
        }
    }
}
