package com.netflix.metacat.converters.impl;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.netflix.metacat.converters.TypeConverter;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by amajumdar on 4/28/15.
 */
public class PrestoTypeConverter implements TypeConverter {
    @Override
    public Type toType(String type, TypeManager typeManager) {
        TypeSignature signature = typeSignatureFromString(type);
        return checkNotNull(typeFromTypeSignature(signature, typeManager), "Invalid type %s", type);
    }

    @Override
    public String fromType(Type type) {
        return type.getDisplayName();
    }

    Type typeFromTypeSignature(TypeSignature typeSignature, TypeManager typeManager) {
        return typeManager.getType(typeSignature);
    }

    TypeSignature typeSignatureFromString(String s) {
        return TypeSignature.parseTypeSignature(s);
    }
}
