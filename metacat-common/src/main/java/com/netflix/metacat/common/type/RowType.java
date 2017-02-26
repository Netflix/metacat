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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Getter;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Row type.
 * @author zhenl
 */
public class RowType extends AbstractType implements ParametricType {
    /** default type. */
    public static final RowType ROW = new RowType(Collections.<Type>emptyList(), Collections.<String>emptyList());

    @Getter
    private final List<RowField> fields;
    /**
     * Constructor.
     * @param fieldTypes fieldTypes
     * @param fieldNames fieldNames
     */
    public RowType(final List<Type> fieldTypes, final List<String> fieldNames) {
        super(new TypeSignature(
                TypeEnum.ROW,
                Lists.transform(fieldTypes, new Function<Type, TypeSignature>() {
                    public TypeSignature apply(@Nullable final Type input) {
                        return input == null ? null : input.getTypeSignature();
                    }
                }),
                 fieldNames == null ? Lists.newArrayList() : Lists.<Object>newArrayList(fieldNames)
            )
        );

        final ImmutableList.Builder<RowField> builder = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            builder.add(new RowField(fieldTypes.get(i), fieldNames.get(i)));
        }
        fields = builder.build();
    }

    @Override
    public TypeEnum getBaseType() {
        return TypeEnum.ROW;
    }

    @Override
    public RowType createType(final List<Type> types, final List<Object> literals) {
        Preconditions.checkArgument(!types.isEmpty(), "types is empty");

        if (literals.isEmpty()) {
            return new RowType(types, Lists.<String>newArrayList());
        }

        Preconditions.checkArgument(types.size() == literals.size(), "types and literals must be matched in size");

        final ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Object literal : literals) {
            builder.add(TypeUtils.checkType(literal, String.class, "literal"));
        }
        return new RowType(types, builder.build());
    }

    /**
     * Row field.
     */
    public static class RowField {
        @Getter private final Type type;
        @Getter private final String name;

        /** constructor.
         * @param type type
         * @param name name
         */
        public RowField(final Type type, final String name) {
            this.type = Preconditions.checkNotNull(type, "type is null");
            this.name = Preconditions.checkNotNull(name, "name is null");
        }

    }

    @Override
    public List<Type> getParameters() {
        final ImmutableList.Builder<Type> result = ImmutableList.builder();
        for (RowField field: fields) {
            result.add(field.getType());
        }
        return result.build();
    }

}
