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

package com.netflix.metacat.common.server.connectors;

import com.netflix.spectator.api.Registry;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Plugin interface implemented by Connectors.
 *
 * @author amajumdar
 * @since 1.0.0
 */
public interface ConnectorPlugin {

    /**
     * Returns the type of the plugin.
     *
     * @return Returns the type of the plugin.
     */
    String getType();

    /**
     * Returns the service implementation for the type.
     *
     * @param connectorName connector name. This is also the catalog name.
     * @param configuration configuration properties
     * @param registry      registry for spectator
     * @return connector factory
     */
    ConnectorFactory create(@Nonnull String connectorName,
                            @Nonnull Map<String, String> configuration,
                            @Nonnull @NonNull Registry registry);


    /**
     * Returns the partition service implementation of the connector.
     *
     * @return Returns the partition service implementation of the connector.
     */
    ConnectorTypeConverter getTypeConverter();

    /**
     * Returns the dto converter implementation of the connector.
     *
     * @return Returns the dto converter implementation of the connector.
     */
    default ConnectorInfoConverter getInfoConverter() {
        return new ConnectorInfoConverter() {
        };
    }
}
