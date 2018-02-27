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

package com.netflix.metacat.connector.s3;

import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.ConnectorPlugin;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.type.TypeRegistry;
import com.netflix.metacat.connector.pig.converters.PigTypeConverter;
import lombok.NonNull;

import javax.annotation.Nonnull;

/**
 * S3 plugin.
 */
public class S3ConnectorPlugin implements ConnectorPlugin {
    /**
     * Type of the connector.
     */
    public static final String CONNECTOR_TYPE = "s3";
    private static final PigTypeConverter PIG_TYPE_CONVERTER = new PigTypeConverter();
    private static final ConnectorInfoConverter INFO_CONVERTER_S3 =
        new S3ConnectorInfoConverter(PIG_TYPE_CONVERTER, true, TypeRegistry.getTypeRegistry());

    /**
     * {@inheritDoc}
     */
    @Override
    public String getType() {
        return CONNECTOR_TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorFactory create(@Nonnull @NonNull final ConnectorContext connectorContext) {
        return new S3ConnectorFactory(connectorContext.getCatalogName(), connectorContext.getCatalogShardName(),
            connectorContext.getConfiguration(), (S3ConnectorInfoConverter) getInfoConverter());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTypeConverter getTypeConverter() {
        return PIG_TYPE_CONVERTER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorInfoConverter getInfoConverter() {
        return INFO_CONVERTER_S3;
    }
}
