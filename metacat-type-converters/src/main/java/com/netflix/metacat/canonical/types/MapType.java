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

package com.netflix.metacat.canonical.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Map type class.
 */

@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
public class MapType extends AbstractType implements ParametricType {
    /**
     * default.
     */
    public static final MapType MAP = new MapType(BaseType.UNKNOWN, BaseType.UNKNOWN);

    private final Type keyType;
    private final Type valueType;

    /**
     * Constructor.
     *
     * @param keyType   keytype
     * @param valueType valuetype
     */
    public MapType(final Type keyType, final Type valueType) {
        super(TypeUtils.parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()));
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public String getDisplayName() {
        return "map<" + keyType.getTypeSignature().toString()
            + ", " + valueType.getTypeSignature().toString() + ">";
    }

    @Override
    public List<Type> getParameters() {
        return ImmutableList.of(getKeyType(), getValueType());
    }

    @Override
    public String getParametricTypeName() {
        return Base.MAP.getBaseTypeDisplayName();
    }

    @Override
    public Type createType(final List<Type> types, final List<Object> literals) {
        Preconditions.checkArgument(types.size() == 2, "Expected two types");
        Preconditions.checkArgument(literals.isEmpty(), "Unexpected literals: %s", literals);
        return new MapType(types.get(0), types.get(1));

    }

}
