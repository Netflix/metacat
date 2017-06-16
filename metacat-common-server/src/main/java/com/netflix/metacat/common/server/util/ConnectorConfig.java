/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.common.server.util;

import com.netflix.metacat.common.server.properties.Config;
import com.netflix.spectator.api.Registry;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

/**
 * Connector Config.
 */
@AllArgsConstructor
@Data
public class ConnectorConfig {
    /**
     * Catalog name.
     */
    private final String catalogName;
    /**
     * Metacat config.
     */
    private final Config config;
    /**
     * The registry for spectator.
     */
    private final Registry registry;
    /**
     * Metacat catalog configuration.
     */
    private final Map<String, String> configuration;
}
