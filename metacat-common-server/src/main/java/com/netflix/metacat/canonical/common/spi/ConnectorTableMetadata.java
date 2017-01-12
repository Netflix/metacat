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

import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
/**
 * connector table metadata.
 */
@Getter
@ToString
@SuppressWarnings("checkstyle:javadocmethod")
public class ConnectorTableMetadata {
    private final SchemaTableName table;
    private final List<ColumnMetadata> columns;
    private final Map<String, Object> properties;
    /* nullable */
    private final String owner;
    private final boolean sampled;

    /**
     * ConnectorTableMetadata.
     * @param table
     * @param columns
     */
    public ConnectorTableMetadata(final SchemaTableName table, final List<ColumnMetadata> columns) {
        this(table, columns, Collections.emptyMap(), null);
    }

    public ConnectorTableMetadata(final SchemaTableName table,
                                  final List<ColumnMetadata> columns, final Map<String, Object> properties) {
        this(table, columns, properties, null);
    }

    public ConnectorTableMetadata(final SchemaTableName table, final List<ColumnMetadata> columns,
                                  final Map<String, Object> properties, final String owner) {
        this(table, columns, properties, owner, false);
    }

    public ConnectorTableMetadata(final SchemaTableName table,
                                  final List<ColumnMetadata> columns,
                                  final Map<String, Object> properties,
                                  final String owner,
                                  final boolean sampled) {
        if (table == null) {
            throw new NullPointerException("table is null or empty");
        }
        if (columns == null) {
            throw new NullPointerException("columns is null");
        }

        this.table = table;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        this.owner = owner;
        this.sampled = sampled;
    }
}
