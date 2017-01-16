/*
 *  Copyright 2016 Netflix, Inc.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *         http://www.apache.org/licenses/LICENSE-2.0
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.canonical.common.spi;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

/**
 * Connector schema metadata.
 */
@Data
@AllArgsConstructor
public class ConnectorSchemaMetadata {
    private String schemaName;
    private String uri;
    private Map<String, String> metadata;

    /**
     * Constructor.
     *
     * @param schemaName schema name
     */
    public ConnectorSchemaMetadata(final String schemaName) {
        this(schemaName, null);
    }

    /**
     * Constructor.
     *
     * @param schemaName schema name
     * @param uri        uri
     */
    public ConnectorSchemaMetadata(final String schemaName, final String uri) {
        this(schemaName, uri, Maps.newHashMap());
    }
}
