package com.netflix.metacat.connector.polaris.store.entities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Converter to represent String Map as a JSON-formatted String in the database.
 */
@Converter
public class StringParamsConverter implements AttributeConverter<Map<String, String>, String> {
    @Override
    public String convertToDatabaseColumn(final Map<String, String> attribute) {
        if (attribute == null || attribute.isEmpty()) {
            return null;
        }
        final TreeMap<String, String> map = new TreeMap<>(attribute);
        try {
            return new ObjectMapper().writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting params Map to String", e);
        }
    }

    @Override
    public Map<String, String> convertToEntityAttribute(final String dbData) {
        if (dbData == null || dbData.isEmpty()) {
            return new HashMap<>();
        }

        try {
            return new ObjectMapper().readValue(dbData, new TypeReference<Map<String, String>>() { });
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting params String to Map", e);
        }
    }
}
