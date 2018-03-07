/*
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
 */

package com.netflix.metacat.connector.hive;

import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorInfoConverter;
import com.netflix.metacat.common.server.connectors.ConnectorPlugin;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;

/**
 * Hive plugin.
 *
 * @author zhenl
 * @since 1.0.0
 */
public class HiveConnectorPlugin implements ConnectorPlugin {
    private static final String CONNECTOR_TYPE = "hive";
    private static final HiveTypeConverter HIVE_TYPE_CONVERTER = new HiveTypeConverter();
    private static final HiveConnectorInfoConverter INFO_CONVERTER_HIVE
        = new HiveConnectorInfoConverter(HIVE_TYPE_CONVERTER);

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
    public ConnectorFactory create(final ConnectorContext connectorContext) {
        return new HiveConnectorFactory(
            INFO_CONVERTER_HIVE,
            connectorContext
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorTypeConverter getTypeConverter() {
        return HIVE_TYPE_CONVERTER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConnectorInfoConverter getInfoConverter() {
        return INFO_CONVERTER_HIVE;
    }
}
