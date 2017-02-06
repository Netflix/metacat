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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.List;


/**
 * Array type class.
 * @author zhenl
 */
public class ArrayType extends AbstractType implements ParametricType {
    /**
     * default.
     */
    public static final ArrayType ARRAY = new ArrayType(BaseType.UNKNOWN);
    @Getter
    private final Type elementType;

    /**
     * Constructor.
     *
     * @param elementType elementtype
     */
    public ArrayType(final Type elementType) {
        super(TypeUtils.parameterizedTypeName("array", elementType.getTypeSignature()));
        this.elementType = Preconditions.checkNotNull(elementType, "elementType is null");
    }

    @Override
    public String getParametricTypeName() {
        return TypeEnum.ARRAY.getBaseTypeDisplayName();
    }

    @Override
    public Type createType(final List<Type> types, final List<Object> literals) {
        Preconditions.checkArgument(types.size() == 1, "Expected only one type, got %s", types);
        Preconditions.checkArgument(literals.isEmpty(), "Unexpected literals: %s", literals);
        return new ArrayType(types.get(0));
    }

    @Override
    public List<Type> getParameters() {
        return ImmutableList.of(getElementType());
    }
}
