package com.netflix.metacat.canonical.type;

import com.google.common.collect.ImmutableList;

import java.util.stream.Collector;

/**
 * Created by zhenli on 12/21/16.
 */
public final class TypeUtils {

    public static TypeSignature parameterizedTypeName(String base, TypeSignature... argumentNames) {
        return new TypeSignature(base, ImmutableList.copyOf(argumentNames), ImmutableList.of());
    }

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
