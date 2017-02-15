package com.netflix.metacat.common.server.converter;

import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.type.Type;
import org.dozer.DozerConverter;

import javax.inject.Inject;
import javax.inject.Provider;

/**
 * Dozer converter implementation for data types.
 */
public class DozerTypeConverter extends DozerConverter<Type, String> {
    private Provider<ConnectorTypeConverter> typeConverter;
    /**
     * Constructor.
     * @param typeConverter Type converter provider
     */
    @Inject
    public DozerTypeConverter(final Provider<ConnectorTypeConverter> typeConverter) {
        super(Type.class, String.class);
        this.typeConverter = typeConverter;
    }

    @Override
    public String convertTo(final Type source, final String destination) {
        return typeConverter.get().fromMetacatType(source);
    }

    @Override
    public Type convertFrom(final String source, final Type destination) {
        return typeConverter.get().toMetacatType(source);
    }
}
