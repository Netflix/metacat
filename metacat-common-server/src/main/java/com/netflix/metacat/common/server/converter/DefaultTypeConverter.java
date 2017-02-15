package com.netflix.metacat.common.server.converter;

import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.type.Type;
import com.netflix.metacat.common.type.TypeRegistry;
import com.netflix.metacat.common.type.TypeSignature;

/**
 * Default type converter. Converter for metacat type representations.
 *
 * @author amajumdar
 */
public class DefaultTypeConverter implements ConnectorTypeConverter {
    @Override
    public Type toMetacatType(final String type) {
        final TypeSignature signature = TypeSignature.parseTypeSignature(type);
        return TypeRegistry.getTypeRegistry().getType(signature);
    }

    @Override
    public String fromMetacatType(final Type type) {
        return type.getTypeSignature().toString();
    }
}
