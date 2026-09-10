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
package com.netflix.metacat.common.type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Renders canonical types into the forms carried on a field DTO. Kept out of the connectors so
 * that DTO producers can reuse them without a connector dependency.
 */
public final class TypeFormatter {

    private static final Map<Type, String> CANONICAL_TO_HIVE = ImmutableMap.<Type, String>builder()
        .put(BaseType.TINYINT, "tinyint")
        .put(BaseType.SMALLINT, "smallint")
        .put(BaseType.INT, "int")
        .put(BaseType.BIGINT, "bigint")
        .put(BaseType.FLOAT, "float")
        .put(BaseType.DOUBLE, "double")
        .put(BaseType.BOOLEAN, "boolean")
        .put(BaseType.STRING, "string")
        .put(VarbinaryType.VARBINARY, "binary")
        .put(BaseType.DATE, "date")
        .put(BaseType.TIMESTAMP, "timestamp")
        .build();

    private TypeFormatter() {
    }

    /**
     * Renders a canonical type in the Hive dialect.
     *
     * @param type canonical type
     * @return the Hive type string, or null if there is no Hive equivalent
     */
    @Nullable
    public static String toHiveString(final Type type) {
        if (CANONICAL_TO_HIVE.containsKey(type)) {
            return CANONICAL_TO_HIVE.get(type);
        }
        if (type instanceof DecimalType || type instanceof CharType || type instanceof VarcharType) {
            return type.getDisplayName();
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.MAP)) {
            final MapType mapType = (MapType) type;
            return "map<" + toHiveString(mapType.getKeyType()) + "," + toHiveString(mapType.getValueType()) + ">";
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.ROW)) {
            final String fields = ((RowType) type).getFields().stream()
                .map(TypeFormatter::rowFieldToHiveString)
                .collect(Collectors.joining(","));
            return "struct<" + fields + ">";
        } else if (type.getTypeSignature().getBase().equals(TypeEnum.ARRAY)) {
            final String elements = ((ParametricType) type).getParameters().stream()
                .map(TypeFormatter::toHiveString)
                .collect(Collectors.joining(","));
            return "array<" + elements + ">";
        }
        return null;
    }

    /**
     * Renders a canonical type as JSON. Only leaf names come from the renderer, so each connector
     * keeps its own naming.
     *
     * @param type     canonical type
     * @param renderer renders a type to its dialect specific name
     * @return the type in JSON form, or null if not renderable
     */
    @Nullable
    public static JsonNode toJson(final Type type, final Function<Type, String> renderer) {
        final TypeEnum base = type.getTypeSignature().getBase();
        if (!base.isParametricType()) {
            return new TextNode(renderer.apply(type));
        } else if (type instanceof DecimalType || type instanceof CharType
            || type instanceof VarcharType || type instanceof VarbinaryType) {
            return parametricPrimitiveToJson(type, renderer);
        } else if (base.equals(TypeEnum.MAP)) {
            final MapType mapType = (MapType) type;
            final ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("type", TypeEnum.MAP.getType());
            node.set("keyType", toJson(mapType.getKeyType(), renderer));
            node.set("valueType", toJson(mapType.getValueType(), renderer));
            return node;
        } else if (base.equals(TypeEnum.ROW)) {
            final ObjectNode node = JsonNodeFactory.instance.objectNode();
            final ArrayNode fieldsNode = node.arrayNode();
            ((RowType) type).getFields().forEach(field -> {
                final ObjectNode fieldNode = JsonNodeFactory.instance.objectNode();
                fieldNode.put("name", field.getName());
                fieldNode.set("type", toJson(field.getType(), renderer));
                fieldsNode.add(fieldNode);
            });
            node.put("type", TypeEnum.ROW.getType());
            node.set("fields", fieldsNode);
            return node;
        } else if (base.equals(TypeEnum.ARRAY)) {
            final ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("type", TypeEnum.ARRAY.getType());
            ((ParametricType) type).getParameters().stream().findFirst()
                .ifPresent(t -> node.set("elementType", toJson(t, renderer)));
            return node;
        }
        return null;
    }

    private static JsonNode parametricPrimitiveToJson(final Type type, final Function<Type, String> renderer) {
        final ObjectNode node = JsonNodeFactory.instance.objectNode();
        final String typeText = renderer.apply(type);
        final int index = typeText.indexOf('(');
        if (index == -1) {
            node.put("type", typeText);
            return node;
        }
        node.put("type", typeText.substring(0, index));
        if (type instanceof DecimalType) {
            node.put("precision", ((DecimalType) type).getPrecision());
            node.put("scale", ((DecimalType) type).getScale());
        } else if (type instanceof CharType) {
            node.put("length", ((CharType) type).getLength());
        } else if (type instanceof VarcharType) {
            node.put("length", ((VarcharType) type).getLength());
        } else {
            node.put("length", ((VarbinaryType) type).getLength());
        }
        return node;
    }

    private static String rowFieldToHiveString(final RowType.RowField rowField) {
        final String prefix = rowField.getName() == null ? "" : rowField.getName() + ":";
        return prefix + toHiveString(rowField.getType());
    }
}
