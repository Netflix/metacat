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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.type.ArrayType;
import com.netflix.metacat.common.type.BaseType;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.MapType;
import com.netflix.metacat.common.type.RowType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeUtils;
import com.netflix.metacat.common.type.VarcharType;
import lombok.NonNull;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Class to convert pig to canonical type and vice versa.
 */
public class PigTypeConverter implements ConnectorTypeConverter {
    private static final String NAME_ARRAY_ELEMENT = "array_element";

    /**
     * {@inheritDoc}.
     */
    @Override
    public Type toMetacatType(@Nonnull @NonNull final String pigType) {
        try {
            final LogicalSchema schema = Utils.parseSchema(pigType);
            final LogicalSchema.LogicalFieldSchema field = schema.getField(0);
            return toCanonicalType(field);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Invalid type signature: '%s'", pigType));
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public String fromMetacatType(@Nonnull @NonNull final Type type) {
        final Schema schema = new Schema(Util.translateFieldSchema(fromCanonicalTypeToPigSchema(null, type)));
        final StringBuilder result = new StringBuilder();
        try {
            Schema.stringifySchema(result, schema, DataType.GENERIC_WRITABLECOMPARABLE, Integer.MIN_VALUE);
        } catch (FrontendException e) {
            throw new IllegalArgumentException(String.format("Invalid for Pig converter: '%s'", type));
        }
        return result.toString();
    }

    private LogicalSchema.LogicalFieldSchema fromCanonicalTypeToPigSchema(final String alias,
                                                                          final Type canonicalType) {
        if (PigTypeMapping.getCANONICAL_TO_PIG().containsKey(canonicalType)) {
            return new LogicalSchema.LogicalFieldSchema(alias,
                null, PigTypeMapping.getCANONICAL_TO_PIG().get(canonicalType));
        } else if (canonicalType instanceof DecimalType) {
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.DOUBLE);
        } else if (canonicalType instanceof VarcharType || canonicalType instanceof CharType) {
            return new LogicalSchema.LogicalFieldSchema(alias, null, DataType.CHARARRAY);
        } else if (canonicalType instanceof MapType) {
            final MapType mapType = (MapType) canonicalType;
            LogicalSchema schema = null;
            if (((MapType) canonicalType).getValueType() != null
                && !BaseType.UNKNOWN.equals(mapType.getValueType())) {
                schema = new LogicalSchema();
                schema.addField(fromCanonicalTypeToPigSchema(alias, mapType.getValueType()));
            }
            return new LogicalSchema.LogicalFieldSchema(alias, schema, DataType.MAP);
        } else if (canonicalType instanceof ArrayType) {
            final ArrayType arrayType = (ArrayType) canonicalType;
            final LogicalSchema schema = new LogicalSchema();
            Type elementType = arrayType.getElementType();
            if (elementType != null) {
                if (!(elementType instanceof RowType)) {
                    elementType = RowType.createRowType(
                        Lists.newArrayList(elementType),
                        ImmutableList.of(NAME_ARRAY_ELEMENT)
                    );
                }
                schema.addField(fromCanonicalTypeToPigSchema(alias, elementType));
            }
            return new LogicalSchema.LogicalFieldSchema(alias, schema, DataType.BAG);

        } else if (canonicalType instanceof RowType) {
            final LogicalSchema schema = new LogicalSchema();
            for (RowType.RowField rowField : ((RowType) canonicalType).getFields()) {
                schema.addField(fromCanonicalTypeToPigSchema(
                    rowField.getName() != null ? rowField.getName() : alias,
                    rowField.getType()));
            }
            return new LogicalSchema.LogicalFieldSchema(alias, schema, DataType.TUPLE);
        }
        throw new IllegalArgumentException(String.format("Invalid for Pig converter: '%s'", canonicalType));
    }

    private Type toCanonicalType(final LogicalSchema.LogicalFieldSchema field) {
        if (PigTypeMapping.getPIG_TO_CANONICAL().containsKey(field.type)) {
            return PigTypeMapping.getPIG_TO_CANONICAL().get(field.type);
        }
        switch (field.type) {
            case DataType.MAP:
                return toCanonicalMapType(field);
            case DataType.BAG:
                return toCanonicalArrayType(field);
            case DataType.TUPLE:
                return toCanonicalRowType(field);
            default:
        }
        throw new IllegalArgumentException(String.format("Invalid for Pig converter: '%s'", field.toString()));
    }

    private Type toCanonicalRowType(final LogicalSchema.LogicalFieldSchema field) {
        final List<Type> fieldTypes = Lists.newArrayList();
        final List<String> fieldNames = Lists.newArrayList();
        for (LogicalSchema.LogicalFieldSchema logicalFieldSchema : field.schema.getFields()) {
            fieldTypes.add(toCanonicalType(logicalFieldSchema));
            fieldNames.add(logicalFieldSchema.alias);
        }
        return RowType.createRowType(fieldTypes, fieldNames);
    }

    private Type toCanonicalArrayType(final LogicalSchema.LogicalFieldSchema field) {
        final LogicalSchema.LogicalFieldSchema subField = field.schema.getField(0);
        Type elementType;
        if (subField.type == DataType.TUPLE
            && !TypeUtils.isNullOrEmpty(subField.schema.getFields())
            && NAME_ARRAY_ELEMENT.equals(subField.schema.getFields().get(0).alias)) {
            elementType = toCanonicalType(subField.schema.getFields().get(0));
        } else {
            elementType = toCanonicalType(subField);
        }
        return new ArrayType(elementType);
    }

    private Type toCanonicalMapType(final LogicalSchema.LogicalFieldSchema field) {
        final Type key = BaseType.STRING;
        Type value = BaseType.UNKNOWN;
        if (null != field.schema && !TypeUtils.isNullOrEmpty(field.schema.getFields())) {
            value = toCanonicalType(field.schema.getFields().get(0));
        }
        return new MapType(key, value);
    }
}
