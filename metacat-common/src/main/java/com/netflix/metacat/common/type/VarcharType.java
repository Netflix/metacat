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

package com.netflix.metacat.common.type;

import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Varchar type.
 *
 * @author zhenl
 */
public final class VarcharType extends AbstractType implements ParametricType {
    /**
     * Default varchar type.
     */
    public static final VarcharType VARCHAR = new VarcharType(1);

    @Getter
    private final int length;

    private VarcharType(final int length) {
        super(
                new TypeSignature(
                        TypeEnum.VARCHAR.getBaseTypeDisplayName(), new ArrayList<TypeSignature>(),
                        Lists.<Object>newArrayList((long) length)));

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

    @Override
    public String getParametricTypeName() {
        return TypeEnum.VARCHAR.getBaseTypeDisplayName();
    }

    @Override
    public Type createType(final List<Type> types, final List<Object> literals) {
        if (literals.isEmpty()) {
            return createVarcharType(1);
        }
        if (literals.size() != 1) {
            throw new IllegalArgumentException("Expected at most one parameter for VARCHAR");
        }
        try {
            return createVarcharType(Integer.parseInt(String.valueOf(literals.get(0))));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("VARCHAR length must be a number");
        }
    }
}
