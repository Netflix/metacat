package com.netflix.metacat.canonical.type;

import com.google.common.collect.ImmutableList;

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


}
