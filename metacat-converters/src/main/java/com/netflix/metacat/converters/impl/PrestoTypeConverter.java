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

package com.netflix.metacat.converters.impl;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Preconditions;
import com.netflix.metacat.converters.TypeConverter;

/**
 * Presto converter.
 */
public class PrestoTypeConverter implements TypeConverter {
    @Override
    public Type toType(final String type, final TypeManager typeManager) {
        final TypeSignature signature = typeSignatureFromString(type);
        return Preconditions.checkNotNull(typeFromTypeSignature(signature, typeManager), "Invalid type %s", type);
    }

    @Override
    public String fromType(final Type type) {
        return type.getDisplayName();
    }

    Type typeFromTypeSignature(final TypeSignature typeSignature, final TypeManager typeManager) {
        return typeManager.getType(typeSignature);
    }

    TypeSignature typeSignatureFromString(final String s) {
        return TypeSignature.parseTypeSignature(s);
    }
}
