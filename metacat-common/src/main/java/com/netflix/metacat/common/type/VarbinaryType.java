/*
 *  Copyright 2017 Netflix, Inc.
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
 */

package com.netflix.metacat.common.type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * VarbinaryType type.
 *
 * @author zhenl
 */
public final class VarbinaryType extends AbstractType implements ParametricType {
    /**
     * Default VarbinaryType type.
     */
    public static final VarbinaryType VARBINARY = new VarbinaryType(Integer.MAX_VALUE);

    @Getter
    private final int length;

    private VarbinaryType(final int length) {
        super(new TypeSignature(
                TypeEnum.VARBINARY,
                new ArrayList<TypeSignature>(),
                Lists.<Object>newArrayList((long) length)));


        if (length < 0) {
            throw new IllegalArgumentException("Invalid VARBINARY length " + length);
        }
        this.length = length;
    }

    /**
     * Creates VarbinaryType.
     *
     * @param length length
     * @return VarcharType
     */
    public static VarbinaryType createVarbinaryType(final int length) {
        return new VarbinaryType(length);
    }

    @Override
    public TypeEnum getBaseType() {
        return TypeEnum.VARBINARY;
    }

    @Override
    public List<Type> getParameters() {
        return ImmutableList.of();
    }

    @Override
    public Type createType(final List<Type> types, final List<Object> literals) {
        if (literals.isEmpty()) {
            return createVarbinaryType(Integer.MAX_VALUE);
        }
        if (literals.size() != 1) {
            throw new IllegalArgumentException("Expected at most one parameter for VARBINARY");
        }
        try {
            return createVarbinaryType(Integer.parseInt(String.valueOf(literals.get(0))));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("VARBINARY length must be a number");
        }
    }
}
