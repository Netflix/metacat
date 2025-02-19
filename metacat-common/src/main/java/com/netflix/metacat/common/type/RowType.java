/*
 *
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
 *
 */
package com.netflix.metacat.common.type;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

/**
 * Row type.
 *
 * @author tgianos
 * @author zhenl
 * @since 1.0.0
 */
@Getter
public class RowType extends AbstractType implements ParametricType {
    /**
     * default type.
     */
    static final RowType ROW = new RowType(Collections.<RowField>emptyList());

    private final List<RowField> fields;

    /**
     * Constructor.
     *
     * @param fields The fields of this row
     */
    public RowType(@Nonnull @NonNull final List<RowField> fields) {
        super(
            new TypeSignature(
                TypeEnum.ROW,
                Lists.transform(
                    Lists.transform(
                        fields,
                        new Function<RowField, Type>() {
                            public Type apply(@Nullable final RowField input) {
                                return input == null ? null : input.getType();
                            }
                        }
                    ),
                    new Function<Type, TypeSignature>() {
                        public TypeSignature apply(@Nullable final Type input) {
                            return input == null ? null : input.getTypeSignature();
                        }
                    }),
                Lists.transform(fields, new Function<RowField, Object>() {
                        public Object apply(@Nullable final RowField input) {
                            return input == null ? null : input.getName();
                        }
                    }
                )
            )
        );

        this.fields = ImmutableList.copyOf(fields);
    }

    /**
     * Create a new Row Type.
     *
     * @param types The types to create can not be empty
     * @param names The literals to use. Can be null but if not must be the same length as types.
     * @return a new RowType
     */
    public static RowType createRowType(
        @Nonnull @NonNull final List<Type> types,
        @Nonnull @NonNull final List<String> names
    ) {
        Preconditions.checkArgument(!types.isEmpty(), "types is empty");

        final ImmutableList.Builder<RowField> builder = ImmutableList.builder();
        Preconditions.checkArgument(
            types.size() == names.size(),
            "types and names must be matched in size"
        );
        for (int i = 0; i < types.size(); i++) {
            builder.add(new RowField(types.get(i), names.get(i)));
        }
        return new RowType(builder.build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypeEnum getBaseType() {
        return TypeEnum.ROW;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowType createType(@Nonnull @NonNull final List<Type> types, @Nonnull @NonNull final List<Object> literals) {
        final ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (final Object literal : literals) {
            builder.add(TypeUtils.checkType(literal, String.class, "literal"));
        }
        return RowType.createRowType(types, builder.build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Type> getParameters() {
        final ImmutableList.Builder<Type> result = ImmutableList.builder();
        for (final RowField field : this.fields) {
            result.add(field.getType());
        }
        return result.build();
    }

    /**
     * Row field.
     */
    @Getter
    @EqualsAndHashCode
    public static class RowField {
        private final Type type;
        private final String name;

        /**
         * constructor.
         *
         * @param type type
         * @param name name
         */
        public RowField(@Nonnull @NonNull final Type type, @Nonnull @NonNull final String name) {
            this.type = type;
            this.name = name;
        }
    }
}
