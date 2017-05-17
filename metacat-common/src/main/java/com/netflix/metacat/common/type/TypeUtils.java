/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.common.type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Collection;

/**
 * Type util class.
 *
 * @author zhenl
 */
public final class TypeUtils {
    private TypeUtils() {
    }

    /**
     * parameterizedTypeName.
     *
     * @param baseType      baseType
     * @param argumentNames args
     * @return type signature
     */
    public static TypeSignature parameterizedTypeSignature(
        final TypeEnum baseType,
        final TypeSignature... argumentNames
    ) {
        return new TypeSignature(baseType, ImmutableList.copyOf(argumentNames), ImmutableList.of());
    }

    /**
     * Check if the collection is null or empty.
     *
     * @param collection collection
     * @return boolean
     */
    public static boolean isNullOrEmpty(final Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    /**
     * CheckType.
     *
     * @param value  value
     * @param target type
     * @param name   name
     * @param <A>    A
     * @param <B>    B
     * @return B
     */
    public static <A, B extends A> B checkType(final A value, final Class<B> target, final String name) {
        Preconditions.checkNotNull(value, "%s is null", name);
        Preconditions.checkArgument(target.isInstance(value),
            "%s must be of type %s, not %s",
            name,
            target.getName(),
            value.getClass().getName());
        return target.cast(value);
    }
}
