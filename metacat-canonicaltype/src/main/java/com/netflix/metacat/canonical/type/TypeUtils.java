package com.netflix.metacat.canonical.type;

import com.google.common.collect.ImmutableList;

/**
 * Created by zhenli on 12/21/16.
 */
public final class TypeUtils {
    public static TypeSignature parameterizedTypeName(String base, TypeSignature... argumentNames)
    {
        return new TypeSignature(base, ImmutableList.copyOf(argumentNames), ImmutableList.of());
    }

}
