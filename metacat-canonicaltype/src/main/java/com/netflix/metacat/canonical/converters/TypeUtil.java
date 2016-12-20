package com.netflix.metacat.canonical.converters;

import com.google.common.base.Preconditions;

/**
 * Created by zhenli on 12/19/16.
 */
public final class TypeUtil {
    private TypeUtil() {
    }

    /**
     * CheckType.
     * @param value value
     * @param target type
     * @param name name
     * @param <A> A
     * @param <B> B
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
