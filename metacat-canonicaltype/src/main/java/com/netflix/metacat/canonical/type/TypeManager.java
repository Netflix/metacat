package com.netflix.metacat.canonical.type;

import java.util.List;

/**
 * Type manager interface
 */
public interface TypeManager {
    /**
     * Gets the type witht the signature, or null if not found.
     * @param signature type signature
     * @return Type
     */
    Type getType(TypeSignature signature);

    /**
     * Get the type with the specified paramenters, or null if not found.
     * @param baseTypeName baseType
     * @param typeParameters typeParameters
     * @param literalParameters literalParameters
     * @return Type
     */
    Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters, List<Object> literalParameters);

    /**
     * Gets a list of all registered types.
     * @return list types.
     */
    List<Type> getTypes();
}

