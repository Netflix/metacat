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

package com.netflix.metacat.connector.pig;

import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorPlugin;
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter;
import com.netflix.metacat.connector.pig.converters.PigTypeConverter;
import com.netflix.spectator.api.Registry;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * S3 plugin.
 */
public class PigConnectorPlugin implements ConnectorPlugin {
    /**
     * Type of the connector.
     */
    public static final String CONNECTOR_TYPE = "pig";
    private static final PigTypeConverter PIG_TYPE_CONVERTER = new PigTypeConverter();

    @Override
    public String getType() {
        return CONNECTOR_TYPE;
    }

    @Override
    public ConnectorFactory create(@Nonnull final String connectorName,
                                   @Nonnull final Map<String, String> configuration,
                                   @Nonnull @NonNull final Registry registry) {
        return null;
    }

    @Override
    public ConnectorTypeConverter getTypeConverter() {
        return PIG_TYPE_CONVERTER;
    }
}
