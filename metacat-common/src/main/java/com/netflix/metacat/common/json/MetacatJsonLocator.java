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
package com.netflix.metacat.common.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * MetacatJson implementation.
 */
public enum MetacatJsonLocator implements MetacatJson {
    /**
     * default metacat JSON instance.
     */
    INSTANCE;

    private final ObjectMapper objectMapper;
    private final ObjectMapper prettyObjectMapper;

    MetacatJsonLocator() {
        objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setSerializationInclusion(JsonInclude.Include.ALWAYS);

        prettyObjectMapper = objectMapper.copy().configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    @Override
    public <T> T convertValue(final Object fromValue, final Class<T> toValueType) throws IllegalArgumentException {
        return objectMapper.convertValue(fromValue, toValueType);
    }

    @Override
    @Nullable
    public ObjectNode deserializeObjectNode(
        @Nonnull final ObjectInputStream inputStream) throws IOException {
        final boolean exists = inputStream.readBoolean();

        ObjectNode json = null;
        if (exists) {
            final String s = inputStream.readUTF();
            json = (ObjectNode) objectMapper.readTree(s);
        }

        return json;
    }

    @Override
    public ObjectNode emptyObjectNode() {
        return objectMapper.createObjectNode();
    }

    @Override
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    @Override
    public ObjectMapper getPrettyObjectMapper() {
        return prettyObjectMapper;
    }

    @Override
    public void mergeIntoPrimary(
        @Nonnull final ObjectNode primary,
        @Nonnull final ObjectNode additional) {
        try {
            recursiveMerge(primary, additional);
        } catch (MetacatJsonException e) {
            throw new IllegalArgumentException("Unable to merge '" + additional + "' into '" + primary + "'");
        }
    }

    @Override
    public ObjectNode parseJsonObject(final String s) {
        final JsonNode node;
        try {
            node = objectMapper.readTree(s);
        } catch (IOException e) {
            throw new MetacatJsonException(e);
        }

        if (node == null || node.isNull()) {
            return null;
        } else if (node.isObject()) {
            return (ObjectNode) node;
        } else {
            throw new MetacatJsonException("Cannot convert '" + s + "' to a json object");
        }
    }

    @Override
    public <T> T parseJsonValue(final String s, final Class<T> clazz) {
        try {
            return objectMapper.readValue(s, clazz);
        } catch (IOException e) {
            throw new MetacatJsonException("Unable to convert '" + s + "' into " + clazz, e);
        }
    }

    @Override
    public <T> T parseJsonValue(final byte[] s, final Class<T> clazz) {
        try {
            return objectMapper.readValue(s, clazz);
        } catch (IOException e) {
            throw new MetacatJsonException("Unable to convert bytes into " + clazz, e);
        }
    }

    private void recursiveMerge(final JsonNode primary, final JsonNode additional) {
        if (!primary.isObject()) {
            throw new MetacatJsonException("This should not be reachable");
        }

        final ObjectNode node = (ObjectNode) primary;

        final Iterator<Map.Entry<String, JsonNode>> fields = additional.fields();
        while (fields.hasNext()) {
            final Map.Entry<String, JsonNode> entry = fields.next();
            final String name = entry.getKey();
            final JsonNode value = entry.getValue();

            // Easiest case, if the primary node doesn't have the current field set the field on the primary
            if (!node.has(name)) {
                node.set(name, value);
            } else if (!value.isObject()) {
                // If the primary has the field but the incoming value is not an object set the field on the primary
                node.set(name, value);
            } else if (!node.get(name).isObject()) {
                // If the primary is currently not an object, just overwrite it with the incoming value
                node.set(name, value);
            } else { // Otherwise recursively merge the new fields from the incoming object into the primary object
                recursiveMerge(node.get(name), value);
            }
        }
    }

    @Override
    public void serializeObjectNode(
        @Nonnull final ObjectOutputStream outputStream,
        @Nullable final ObjectNode json)
        throws IOException {
        final boolean exists = json != null;
        outputStream.writeBoolean(exists);
        if (exists) {
            outputStream.writeUTF(json.toString());
        }
    }

    @Override
    public byte[] toJsonAsBytes(final Object o) {
        try {
            return objectMapper.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new MetacatJsonException(e);
        }
    }

    @Override
    public ObjectNode toJsonObject(final Object o) {
        return objectMapper.valueToTree(o);
    }

    @Override
    public String toJsonString(final Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new MetacatJsonException(e);
        }
    }
}
