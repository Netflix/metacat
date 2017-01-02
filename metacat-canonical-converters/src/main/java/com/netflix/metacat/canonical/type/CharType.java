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
            return createCharType(Integer.parseInt(String.valueOf(literals.get(0))));
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
