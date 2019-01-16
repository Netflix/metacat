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
     * @param connectorContext      registry for micrometer
     * @return connector factory
     */
    ConnectorFactory create(ConnectorContext connectorContext);

    /**
     * Returns the type convertor of the connector.
     *
     * @return Returns the type convertor of the connector.
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
