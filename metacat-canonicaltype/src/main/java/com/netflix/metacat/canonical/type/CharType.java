package com.netflix.metacat.canonical.type;

import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.List;

/**
 * CharType.
 */
@EqualsAndHashCode(callSuper = true)
public class CharType extends AbstractType implements ParametricType {
    /** Default character type. */
    public static final CharType CHAR = new CharType(1);

    private final int length;

    /**
     * Constructor.
     * @param length length
     */
    public CharType(final int length) {
        super(
            new TypeSignature(
                Base.CHAR.getBaseTypeDisplayName(), Lists.newArrayList(),
                Collections.singletonList((long) length)));

        if (length < 0) {
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        this.length = length;
    }

    @Override
    public String getParametricTypeName() {
        return Base.CHAR.getBaseTypeDisplayName();
    }

    @Override
    public Type createType(final List<Type> types, final List<Object> literals) {
        if (literals.isEmpty()) {
            return createCharType(1);
        }
        if (literals.size() != 1) {
            throw new IllegalArgumentException("Expected at most one parameter for CHAR");
        }
        try {
            return createCharType(Integer.valueOf(String.valueOf(literals.get(0))));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("CHAR length must be a number");
        }
    }

    /**
     * Creates the character type.
     * @param length legnth of the type
     * @return CharType
     */
    public static CharType createCharType(final int length) {
        return new CharType(length);
    }

}
