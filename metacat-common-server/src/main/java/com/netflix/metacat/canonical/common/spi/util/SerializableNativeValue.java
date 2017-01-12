package com.netflix.metacat.canonical.common.spi.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Serializable Native Value class.
 */
@JsonSerialize(using = SerializableNativeValue.Serializer.class)
@JsonDeserialize(using = SerializableNativeValue.Deserializer.class)
@Getter
@EqualsAndHashCode
public final class SerializableNativeValue {
    private final Class<?> type;
    private final Comparable<?> value;

    /**
     * constructor.
     * @param type type
     * @param value value
     */
    public SerializableNativeValue(final Class<?> type, final Comparable<?> value) {
        this.type = Objects.requireNonNull(type, "type is null");
        this.value = value;
        if (value != null && !type.isInstance(value)) {
            throw new IllegalArgumentException(String.format("type %s does not match value %s",
                type.getClass(), value));
        }
    }

    /**
     * serializer class.
     */
    public static class Serializer
        extends JsonSerializer<SerializableNativeValue> {
        @Override
        public void serialize(final SerializableNativeValue value,
                              final JsonGenerator generator, final SerializerProvider provider)
            throws IOException {
            generator.writeStartObject();
            generator.writeStringField("type", value.getType().getName());
            generator.writeFieldName("value");
            if (value.getValue() == null) {
                generator.writeNull();
            } else {
                writeValue(value, generator);
            }
            generator.writeEndObject();
        }

        private static void writeValue(final SerializableNativeValue value,
                                       final JsonGenerator jsonGenerator)
            throws IOException {
            final Class<?> type = value.getType();
            if (type == String.class) {
                jsonGenerator.writeString((String) value.getValue());
            } else if (type == Slice.class) {
                jsonGenerator.writeString(((Slice) value.getValue()).toStringUtf8());
            } else if (type == Boolean.class) {
                jsonGenerator.writeBoolean((Boolean) value.getValue());
            } else if (type == Long.class) {
                jsonGenerator.writeNumber((Long) value.getValue());
            } else if (type == Double.class) {
                jsonGenerator.writeNumber((Double) value.getValue());
            } else {
                throw new AssertionError("Unknown type: " + type);
            }
        }
    }

    /**
     * Deserializer class.
     */
    public static class Deserializer
        extends JsonDeserializer<SerializableNativeValue> {
        @Override
        public SerializableNativeValue deserialize(final JsonParser jsonParser, final DeserializationContext context)
            throws IOException {
            checkJson(jsonParser.nextFieldName(new SerializedString("type")));

            final String typeString = jsonParser.nextTextValue();
            final Class<?> type = extractClassType(typeString);

            checkJson(jsonParser.nextFieldName(new SerializedString("value")));

            final JsonToken token = jsonParser.nextToken();
            final Comparable<?> value = (token == JsonToken.VALUE_NULL) ? null : readValue(type, jsonParser);
            checkJson(jsonParser.nextToken() == JsonToken.END_OBJECT);

            return new SerializableNativeValue(type, value);
        }

        private static Comparable<?> readValue(final Class<?> type, final JsonParser jsonParser)
            throws IOException {
            if (type == String.class) {
                final String value = jsonParser.getValueAsString();
                checkJson(value != null);
                return value;
            } else if (type == Slice.class) {
                final String value = jsonParser.getValueAsString();
                checkJson(value != null);
                return Slices.copiedBuffer(value, StandardCharsets.UTF_8);
            } else if (type == Boolean.class) {
                return jsonParser.getBooleanValue();
            } else if (type == Long.class) {
                return jsonParser.getLongValue();
            } else if (type.equals(Double.class)) {
                return jsonParser.getDoubleValue();
            } else {
                throw new AssertionError("Unknown type: " + type);
            }
        }

        private static void checkJson(final boolean condition) {
            if (!condition) {
                throw new IllegalArgumentException("Malformed SerializableNativeValue JSON object");
            }
        }

        private static Class<?> extractClassType(final String typeString) {
            try {
                return Class.forName(typeString);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Unknown class type: " + typeString);
            }
        }
    }
}
