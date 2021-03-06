/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.google.common.collect.Maps;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Type converter factory.
 *
 * @author amajumdar
 * @author tgianos
 * @since 1.0.0
 */
public class TypeConverterFactory {
    private static final String METACAT_TYPE_CONTEXT = "metacat";
    private Map<String, ConnectorTypeConverter> registry = Maps.newHashMap();
    private ConnectorTypeConverter defaultTypeConverter;

    /**
     * Constructor.
     *
     * @param defaultTypeConverter default type converter
     */
    public TypeConverterFactory(final ConnectorTypeConverter defaultTypeConverter) {
        this.defaultTypeConverter = defaultTypeConverter;
        this.registry.put(METACAT_TYPE_CONTEXT, new DefaultTypeConverter());
    }

    /**
     * Adds the type converter to the registry.
     *
     * @param connectorType connector type
     * @param typeConverter types converter
     */
    public void register(final String connectorType, final ConnectorTypeConverter typeConverter) {
        this.registry.put(connectorType, typeConverter);
    }

    /**
     * Returns the right type converter based on the context.
     *
     * @param context context
     * @return type converter
     */
    public ConnectorTypeConverter get(@Nullable final String context) {
        if (context == null) {
            return defaultTypeConverter;
        }
        final ConnectorTypeConverter result = this.registry.get(context);
        if (result == null) {
            throw new IllegalArgumentException("No handler for " + context);
        }
        return result;
    }
}
