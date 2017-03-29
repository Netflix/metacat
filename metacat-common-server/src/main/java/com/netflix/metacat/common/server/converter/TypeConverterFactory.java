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

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import org.springframework.beans.factory.FactoryBean;

import java.util.Map;

/**
 * Type converter factory.
 *
 * @author amajumdar
 * @author tgianos
 * @since 1.0.0
 */
public class TypeConverterFactory implements FactoryBean<ConnectorTypeConverter> {

    private final Config config;
    private Map<String, ConnectorTypeConverter> registry = Maps.newHashMap();

    /**
     * Constructor.
     *
     * @param config config
     */
    public TypeConverterFactory(final Config config) {
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTypeConverter getObject() {
        final MetacatRequestContext requestContext = MetacatContextManager.getContext();
        final String dataTypeContext = requestContext.getDataTypeContext();

        if (dataTypeContext == null) {
            return this.getDefaultConverter();
        } else {
            return this.get(dataTypeContext);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?> getObjectType() {
        return ConnectorTypeConverter.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSingleton() {
        return false;
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
    public ConnectorTypeConverter get(final String context) {
        final ConnectorTypeConverter result = this.registry.get(context);
        if (result == null) {
            throw new IllegalArgumentException("No handler for " + context);
        }
        return result;
    }

    private ConnectorTypeConverter getDefaultConverter() {
        try {
            return (ConnectorTypeConverter) Class.forName(this.config.getDefaultTypeConverter()).newInstance();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
