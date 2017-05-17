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
package com.netflix.metacat.common.server.converter;

import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.common.type.Type;
import lombok.NonNull;
import org.dozer.DozerConverter;

import javax.annotation.Nonnull;

/**
 * Dozer converter implementation for data types.
 *
 * @author amajumdar
 * @author tgianos
 * @since 1.0.0
 */
public class DozerTypeConverter extends DozerConverter<Type, String> {
    private TypeConverterFactory typeConverterFactory;

    /**
     * Constructor.
     *
     * @param typeConverterFactory Type converter factory
     */
    public DozerTypeConverter(@Nonnull @NonNull final TypeConverterFactory typeConverterFactory) {
        super(Type.class, String.class);
        this.typeConverterFactory = typeConverterFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String convertTo(final Type source, final String destination) {
        final ConnectorTypeConverter typeConverter;
        try {
            typeConverter = this.typeConverterFactory.get(MetacatContextManager.getContext().getDataTypeContext());
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to get a type converter", e);
        }
        return typeConverter.fromMetacatType(source);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Type convertFrom(final String source, final Type destination) {
        final ConnectorTypeConverter typeConverter;
        try {
            typeConverter = this.typeConverterFactory.get(MetacatContextManager.getContext().getDataTypeContext());
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to get a type converter", e);
        }

        return typeConverter.toMetacatType(source);
    }
}
