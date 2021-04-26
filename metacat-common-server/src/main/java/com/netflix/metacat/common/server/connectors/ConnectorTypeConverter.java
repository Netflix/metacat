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
package com.netflix.metacat.common.server.connectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.netflix.metacat.common.json.MetacatJsonLocator;
import com.netflix.metacat.common.type.CharType;
import com.netflix.metacat.common.type.DecimalType;
import com.netflix.metacat.common.type.MapType;
import com.netflix.metacat.common.type.ParametricType;
import com.netflix.metacat.common.type.RowType;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeEnum;
import com.netflix.metacat.common.type.VarbinaryType;
import com.netflix.metacat.common.type.VarcharType;

/**
 * Canonical type converter class.
 *
 * @author tgianos
 * @author zhenl
 * @since 1.0.0
 */
public interface ConnectorTypeConverter {

    /**
     * Converts to metacat type.
     *
     * @param type type
     * @return metacat type
     */
    Type toMetacatType(String type);

    /**
     * Converts from metacat type.
     *
     * @param type type
     * @return connector type
     */
    String fromMetacatType(Type type);

    /**
     * Converts from Metacat type to JSON format.
     * @param type type
     * @return Type in JSON format
     */
    default JsonNode fromMetacatTypeToJson(Type type) {
        final MetacatJsonLocator json = new MetacatJsonLocator();
        JsonNode result = null;
        final TypeEnum base = type.getTypeSignature().getBase();
        if (!base.isParametricType()) {
            result =  new TextNode(fromMetacatType(type));
        } else if (type instanceof DecimalType || type instanceof CharType
            || type instanceof VarcharType || type instanceof VarbinaryType) {
            final ObjectNode node = json.emptyObjectNode();
            final String typeText = fromMetacatType(type);
            final int index = typeText.indexOf('(');
            if (index == -1) {
                node.put("type", typeText);
            } else {
                node.put("type", typeText.substring(0, index));
                if (type instanceof DecimalType) {
                    node.put("precision", ((DecimalType) type).getPrecision());
                    node.put("scale", ((DecimalType) type).getScale());
                } else if (type instanceof CharType) {
                    node.put("length", ((CharType) type).getLength());
                } else if (type instanceof  VarcharType) {
                    node.put("length", ((VarcharType) type).getLength());
                } else {
                    node.put("length", ((VarbinaryType) type).getLength());
                }
            }
            result = node;
        } else if (base.equals(TypeEnum.MAP)) {
            final MapType mapType = (MapType) type;
            final ObjectNode node = json.emptyObjectNode();
            node.put("type", TypeEnum.MAP.getType());
            node.set("keyType", fromMetacatTypeToJson(mapType.getKeyType()));
            node.set("valueType", fromMetacatTypeToJson(mapType.getValueType()));
            result =   node;
        } else if (base.equals(TypeEnum.ROW)) {
            final RowType rowType = (RowType) type;
            final ObjectNode node = json.emptyObjectNode();
            final ArrayNode fieldsNode = node.arrayNode();
            rowType.getFields().forEach(f -> {
                final ObjectNode fieldNode = json.emptyObjectNode();
                fieldNode.put("name", f.getName());
                fieldNode.set("type", fromMetacatTypeToJson(f.getType()));
                fieldsNode.add(fieldNode);
            });
            node.put("type", TypeEnum.ROW.getType());
            node.set("fields", fieldsNode);
            result = node;
        } else if (base.equals(TypeEnum.ARRAY)) {
            final ObjectNode node = json.emptyObjectNode();
            node.put("type", TypeEnum.ARRAY.getType());
            ((ParametricType) type).getParameters().stream().findFirst()
                .ifPresent(t -> node.set("elementType", fromMetacatTypeToJson(t)));
            result = node;
        }
        return result;
    }
}
