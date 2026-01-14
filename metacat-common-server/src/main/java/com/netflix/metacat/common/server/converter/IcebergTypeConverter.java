/*
 * Copyright 2017 Netflix, Inc.
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
 */
package com.netflix.metacat.common.server.converter;

import com.google.common.collect.ImmutableList;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeEnum;
import com.netflix.metacat.common.type.TypeRegistry;
import com.netflix.metacat.common.type.TypeSignature;
import com.netflix.metacat.common.type.VarbinaryType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converter for Iceberg types to Metacat types.
 * Provides direct conversion without going through Hive type strings.
 *
 * @author metacat team
 * @since 1.4.0
 */
public class IcebergTypeConverter implements ConnectorTypeConverter {

    /**
     * Converts an Iceberg type to a Metacat type.
     *
     * @param icebergType the Iceberg type
     * @return the corresponding Metacat type
     */
    public Type toMetacatType(final org.apache.iceberg.types.Type icebergType) {
        switch (icebergType.typeId()) {
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
                return BaseType.TIME;
            case TIMESTAMP:
                return BaseType.TIMESTAMP;
            case STRING:
            case UUID:
                return BaseType.STRING;
            case FIXED:
            case BINARY:
                return VarbinaryType.VARBINARY;
            case DECIMAL:
                final Types.DecimalType decimalType = (Types.DecimalType) icebergType;
                return DecimalType.createDecimalType(decimalType.precision(), decimalType.scale());
            case STRUCT:
                return convertStructType(icebergType.asStructType());
            case LIST:
                return convertListType(icebergType.asListType());
            case MAP:
                return convertMapType(icebergType.asMapType());
            default:
                throw new UnsupportedOperationException("Unsupported Iceberg type: " + icebergType);
        }
    }

    private Type convertStructType(final Types.StructType structType) {
        final List<TypeSignature> fieldTypes = new ArrayList<>();
        final List<Object> fieldNames = new ArrayList<>();

        for (Types.NestedField field : structType.fields()) {
            fieldNames.add(field.name());
            fieldTypes.add(toMetacatType(field.type()).getTypeSignature());
        }

        return TypeRegistry.getTypeRegistry().getParameterizedType(
            TypeEnum.ROW, fieldTypes, fieldNames);
    }

    private Type convertListType(final Types.ListType listType) {
        final Type elementType = toMetacatType(listType.elementType());
        return TypeRegistry.getTypeRegistry().getParameterizedType(
            TypeEnum.ARRAY,
            ImmutableList.of(elementType.getTypeSignature()),
            ImmutableList.of());
    }

    private Type convertMapType(final Types.MapType mapType) {
        final Type keyType = toMetacatType(mapType.keyType());
        final Type valueType = toMetacatType(mapType.valueType());
        return TypeRegistry.getTypeRegistry().getParameterizedType(
            TypeEnum.MAP,
            ImmutableList.of(keyType.getTypeSignature(), valueType.getTypeSignature()),
            ImmutableList.of());
    }

    /**
     * Converts an Iceberg schema to a list of FieldInfo.
     *
     * @param schema          the Iceberg schema
     * @param partitionFields the partition fields
     * @return list of FieldInfo
     */
    public List<FieldInfo> icebergSchemaToFieldInfo(final Schema schema,
                                                    final List<PartitionField> partitionFields) {
        final List<FieldInfo> fields = new ArrayList<>();
        final List<String> partitionNames = partitionFields.stream()
            .filter(f -> f.transform() != null && !f.transform().toString().equalsIgnoreCase("void"))
            .map(f -> schema.findField(f.sourceId()).name())
            .collect(Collectors.toList());

        for (Types.NestedField field : schema.columns()) {
            final FieldInfo fieldInfo = FieldInfo.builder()
                .name(field.name())
                .sourceType(field.type().toString())
                .type(toMetacatType(field.type()))
                .isNullable(field.isOptional())
                .comment(field.doc())
                .partitionKey(partitionNames.contains(field.name()))
                .build();
            fields.add(fieldInfo);
        }

        return fields;
    }

    @Override
    public Type toMetacatType(final String type) {
        // This method is part of ConnectorTypeConverter interface
        // For Iceberg, we primarily use toMetacatType(org.apache.iceberg.types.Type)
        // This string-based method is not the primary use case
        throw new UnsupportedOperationException(
            "Use toMetacatType(org.apache.iceberg.types.Type) for Iceberg type conversion");
    }

    @Override
    public String fromMetacatType(final Type type) {
        // Convert Metacat type back to Iceberg type string representation
        // This is used for display/serialization purposes
        if (type instanceof BaseType) {
            return fromBaseType((BaseType) type);
        } else if (type instanceof DecimalType) {
            final DecimalType decimalType = (DecimalType) type;
            return String.format("decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.ARRAY)) {
            final Type elementType = TypeRegistry.getTypeRegistry().getType(
                type.getTypeSignature().getParameters().get(0));
            return "list<" + fromMetacatType(elementType) + ">";
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.MAP)) {
            final Type keyType = TypeRegistry.getTypeRegistry().getType(
                type.getTypeSignature().getParameters().get(0));
            final Type valueType = TypeRegistry.getTypeRegistry().getType(
                type.getTypeSignature().getParameters().get(1));
            return "map<" + fromMetacatType(keyType) + "," + fromMetacatType(valueType) + ">";
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.ROW)) {
            return "struct<...>"; // Simplified for display
        }
        return type.getDisplayName();
    }

    private String fromBaseType(final BaseType type) {
        switch (type.getTypeSignature().getBase()) {
            case BOOLEAN:
                return "boolean";
            case INT:
                return "int";
            case BIGINT:
                return "long";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            case DATE:
                return "date";
            case TIME:
                return "time";
            case TIMESTAMP:
                return "timestamp";
            default:
                return type.getDisplayName();
        }
    }
}
