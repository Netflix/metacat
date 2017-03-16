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
import com.netflix.metacat.common.type.Type;
import org.dozer.DozerConverter;

import javax.inject.Inject;
import javax.inject.Provider;

/**
 * Dozer converter implementation for data types.
 */
public class DozerTypeConverter extends DozerConverter<Type, String> {
    private Provider<ConnectorTypeConverter> typeConverter;
    /**
     * Constructor.
     * @param typeConverter Type converter provider
     */
    @Inject
    public DozerTypeConverter(final Provider<ConnectorTypeConverter> typeConverter) {
        super(Type.class, String.class);
        this.typeConverter = typeConverter;
    }

    @Override
    public String convertTo(final Type source, final String destination) {
        return typeConverter.get().fromMetacatType(source);
    }

    @Override
    public Type convertFrom(final String source, final Type destination) {
        return typeConverter.get().toMetacatType(source);
    }
}
