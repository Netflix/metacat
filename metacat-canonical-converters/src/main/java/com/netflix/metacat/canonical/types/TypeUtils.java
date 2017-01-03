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

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.stream.Collector;

/**
 * Created by zhenli on 12/21/16.
 */
public final class TypeUtils {
    private TypeUtils() {
    }

    /**
     * parameterizedTypeName.
     *
     * @param base          base
     * @param argumentNames args
     * @return typesignature
     */
    public static TypeSignature parameterizedTypeName(final String base, final TypeSignature... argumentNames) {
        return new TypeSignature(base, ImmutableList.copyOf(argumentNames), ImmutableList.of());
    }

    /**
     * collector util.
     *
     * @param <T> t
     * @return type.
     */
    public static <T> Collector<T, ?, ImmutableList<T>> toImmutableList() {
        return Collector.<T, ImmutableList.Builder<T>, ImmutableList<T>>of(
            ImmutableList.Builder::new,
            ImmutableList.Builder::add,
            (left, right) -> {
                left.addAll(right.build());
                return left;
            },
            ImmutableList.Builder::build);
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
}
