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

package com.facebook.presto.spi;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by amajumdar on 3/9/16.
 */
public class ConnectorSchemaMetadata {
    private String schemaName;
    private String uri;
    private Map<String, String> metadata;

    public ConnectorSchemaMetadata(String schemaName) {
        this(schemaName, null);
    }

    public ConnectorSchemaMetadata(String schemaName, String uri) {
        this(schemaName, uri, Maps.newHashMap());
    }

    public ConnectorSchemaMetadata(String schemaName, String uri, Map<String, String> metadata) {
        this.schemaName = schemaName;
        this.uri = uri;
        this.metadata = metadata;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getUri() {
        return uri;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }
}
