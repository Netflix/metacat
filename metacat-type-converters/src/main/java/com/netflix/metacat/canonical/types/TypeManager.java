/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.canonical.types;

import java.util.List;

/**
 * Type manager interface.
 * @author zhenl
 */
public interface TypeManager {
    /**
     * Gets the type witht the signature, or null if not found.
     *
     * @param signature type signature
     * @return Type
     */
    Type getType(TypeSignature signature);

    /**
     * Get the type with the specified paramenters, or null if not found.
     *
     * @param baseTypeName      baseType
     * @param typeParameters    typeParameters
     * @param literalParameters literalParameters
     * @return Type
     */
    Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters, List<Object> literalParameters);

    /**
     * Gets a list of all registered types.
     *
     * @return list types.
     */
    List<Type> getTypes();
}

