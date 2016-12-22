package com.netflix.metacat.canonical.type;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.Collections;

/**
 * Created by zhenli on 12/21/16.
 */
public final class VarcharType extends AbstractType {
    /**
     * Default varchar type.
     */
    public static final VarcharType VARCHAR = new VarcharType(1);

    @Getter
    private final int length;

    private VarcharType(final int length) {
        super(
            new TypeSignature(
                Base.VARCHAR.getBaseTypeDisplayName(), Lists.newArrayList(),
                Collections.singletonList((long) length)));

        if (length < 0) {
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        this.length = length;
    }

    /**
     * Cretes varchar type.
     *
     * @param length length
     * @return VarcharType
     */
    public static VarcharType createVarcharType(final int length) {
        return new VarcharType(length);
    }
}
