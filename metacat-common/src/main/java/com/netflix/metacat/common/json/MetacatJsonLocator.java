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

public enum MetacatJsonLocator implements MetacatJson {
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
    public <T> T convertValue(Object fromValue, Class<T> toValueType) throws IllegalArgumentException {
        return objectMapper.convertValue(fromValue, toValueType);
    }

    @Override
    @Nullable
    public ObjectNode deserializeObjectNode(@Nonnull ObjectInputStream inputStream) throws IOException {
        boolean exists = inputStream.readBoolean();

        ObjectNode json = null;
        if (exists) {
            String s = inputStream.readUTF();
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
    public void mergeIntoPrimary(@Nonnull ObjectNode primary, @Nonnull ObjectNode additional) {
        try {
            recursiveMerge(primary, additional);
        } catch (MetacatJsonException e) {
            throw new IllegalArgumentException("Unable to merge '" + additional + "' into '" + primary + "'");
        }
    }

    @Override
    public ObjectNode parseJsonObject(String s) {
        JsonNode node;
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
    public <T> T parseJsonValue(String s, Class<T> clazz) {
        try {
            return objectMapper.readValue(s, clazz);
        } catch (IOException e) {
            throw new MetacatJsonException("Unable to convert '" + s + "' into " + clazz, e);
        }
    }

    @Override
    public <T> T parseJsonValue(byte[] s, Class<T> clazz) {
        try {
            return objectMapper.readValue(s, clazz);
        } catch (IOException e) {
            throw new MetacatJsonException("Unable to convert bytes into " + clazz, e);
        }
    }

    private void recursiveMerge(JsonNode primary, JsonNode additional) {
        if (!primary.isObject()) {
            throw new MetacatJsonException("This should not be reachable");
        }

        ObjectNode node = (ObjectNode) primary;

        Iterator<Map.Entry<String, JsonNode>> fields = additional.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String name = entry.getKey();
            JsonNode value = entry.getValue();

            // Easiest case, if the primary node doesn't have the current field set the field on the primary
            if (!node.has(name)) {
                node.set(name, value);
            }
            // If the primary has the field but the incoming value is not an object set the field on the primary
            else if (!value.isObject()) {
                node.set(name, value);
            }
            // If the primary is currently not an object, just overwrite it with the incoming value
            else if (!node.get(name).isObject()) {
                node.set(name, value);
            }
            // Otherwise recursively merge the new fields from the incoming object into the primary object
            else {
                recursiveMerge(node.get(name), value);
            }
        }
    }

    @Override
    public void serializeObjectNode(@Nonnull ObjectOutputStream outputStream, @Nullable ObjectNode json)
            throws IOException {
        boolean exists = json != null;
        outputStream.writeBoolean(exists);
        if (exists) {
            outputStream.writeUTF(json.toString());
        }
    }

    @Override
    public byte[] toJsonAsBytes(Object o) {
        try {
            return objectMapper.writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new MetacatJsonException(e);
        }
    }

    @Override
    public ObjectNode toJsonObject(Object o) {
        return objectMapper.valueToTree(o);
    }

    @Override
    public String toJsonString(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new MetacatJsonException(e);
        }
    }
}
