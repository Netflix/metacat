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
import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.netflix.metacat.type.converters.TypeUtil;

/**
 * Created by zhenli on 12/20/16.
 */
public class RowType extends AbstractType implements ParametricType {
    /** default type. */
    public static final RowType ROW = new RowType(Collections.emptyList(), Optional.empty());

    @Getter
    private final List<RowField> fields;
    /**
     * Constructor.
     * @param fieldTypes fieldTypes
     * @param fieldNames fieldNames
     */
    public RowType(final List<Type> fieldTypes, final Optional<List<String>> fieldNames) {
        super(new TypeSignature(
                "row",
                Lists.transform(fieldTypes, Type::getTypeSignature),
                 fieldNames.orElse(ImmutableList.of()).stream()
                        .map(Object.class::cast)
                        .collect(TypeUtils.toImmutableList())
            )
        );

        final ImmutableList.Builder<RowField> builder = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            final int index = i;
            builder.add(new RowField(fieldTypes.get(i), fieldNames.map((names) -> names.get(index))));
        }
        fields = builder.build();
    }

    @Override
    public String getParametricTypeName() {
        return Base.ROW.getBaseTypeDisplayName();
    }

    @Override
    public RowType createType(final List<Type> types, final List<Object> literals) {
        Preconditions.checkArgument(!types.isEmpty(), "types is empty");

        if (literals.isEmpty()) {
            return new RowType(types, Optional.empty());
        }

        Preconditions.checkArgument(types.size() == literals.size(), "types and literals must be matched in size");

        final ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Object literal : literals) {
            builder.add(TypeUtil.checkType(literal, String.class, "literal"));
        }
        return new RowType(types, Optional.of(builder.build()));
    }

    @Override
    public List<Type> getParameters() {
         return fields.stream()
            .map(RowField::getType)
            .collect(TypeUtils.toImmutableList());
    }

    /**
     * Row field.
     */
    public static class RowField {
        @Getter private final Type type;
        @Getter private final Optional<String> name;

        /** constructor.
         * @param type type
         * @param name name
         */
        public RowField(final Type type, final Optional<String> name) {
            this.type = Preconditions.checkNotNull(type, "type is null");
            this.name = Preconditions.checkNotNull(name, "name is null");
        }

    }

}
