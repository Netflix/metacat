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
