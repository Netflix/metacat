package com.netflix.metacat.common.server.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.common.type.Type;
import lombok.NonNull;
import org.dozer.CustomConverter;

import javax.annotation.Nonnull;

/**
 * Dozer converter implementation to convert data types to JSON format.
 *
 * @author amajumdar
 */
public class DozerJsonTypeConverter implements CustomConverter {
    private TypeConverterFactory typeConverterFactory;

    /**
     * Constructor.
     *
     * @param typeConverterFactory Type converter factory
     */
    public DozerJsonTypeConverter(@Nonnull @NonNull final TypeConverterFactory typeConverterFactory) {
        this.typeConverterFactory = typeConverterFactory;
    }

    @Override
    public Object convert(final Object existingDestinationFieldValue,
                          final Object sourceFieldValue,
                          final Class<?> destinationClass,
                          final Class<?> sourceClass) {
        JsonNode result = null;
        final Type type = (Type) sourceFieldValue;
        final ConnectorTypeConverter typeConverter;
        try {
            typeConverter = this.typeConverterFactory.get(MetacatContextManager.getContext().getDataTypeContext());
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to get a type converter", e);
        }
        try {
            result = typeConverter.fromMetacatTypeToJson(type);
        } catch (final Exception ignored) {
            // TODO: Handle exception.
        }
        return result;
    }
}
