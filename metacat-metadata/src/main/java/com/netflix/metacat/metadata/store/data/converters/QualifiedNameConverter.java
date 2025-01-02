package com.netflix.metacat.metadata.store.data.converters;

import com.netflix.metacat.common.QualifiedName;
import lombok.extern.slf4j.Slf4j;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

/**
 * The attribute converter for the QualifiedName type.
 *
 * @author rveeramacheneni
 */
@Slf4j
@Converter(autoApply = true)
@SuppressWarnings("PMD")
public class QualifiedNameConverter implements AttributeConverter<QualifiedName, String> {

    @Override
    public String convertToDatabaseColumn(final QualifiedName attribute) {
        return attribute == null ? null : attribute.toString();
    }

    @Override
    public QualifiedName convertToEntityAttribute(final String dbData) {
        return dbData == null ? null : QualifiedName.fromString(dbData);
    }
}
