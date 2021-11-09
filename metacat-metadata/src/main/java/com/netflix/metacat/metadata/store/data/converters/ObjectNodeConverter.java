package com.netflix.metacat.metadata.store.data.converters;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.metacat.common.json.MetacatJson;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/**
 * Attribute converter for Jackson ObjectNode type.
 *
 * @author rveeramacheneni
 */
@Slf4j
@Converter(autoApply = true)
@SuppressWarnings("PMD")
public class ObjectNodeConverter implements AttributeConverter<ObjectNode, String> {
    private final MetacatJson metacatJson;

    /**
     * Ctor.
     *
     * @param metacatJson the Jackson object mapper.
     */
    public ObjectNodeConverter(@NonNull final MetacatJson metacatJson) {
        this.metacatJson = metacatJson;
    }

    @Override
    public String convertToDatabaseColumn(final ObjectNode attribute) {
        return attribute == null ? null : attribute.toString();
    }

    @Override
    public ObjectNode convertToEntityAttribute(final String dbData) {
        return dbData == null ? null : metacatJson.parseJsonObject(dbData);
    }
}
