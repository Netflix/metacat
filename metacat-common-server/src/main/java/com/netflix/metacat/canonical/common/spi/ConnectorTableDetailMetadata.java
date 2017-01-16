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
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * This class contains any extra metadata about the table. This was created initially for the serde info that exists in
 * hive.
 */
@Getter
@Setter
public class ConnectorTableDetailMetadata extends ConnectorTableMetadata {
    private StorageInfo storageInfo;
    private Map<String, String> metadata;
    private AuditInfo auditInfo;

    /**
     * Constructor.
     *
     * @param table    table name
     * @param columns  list of columns
     * @param metadata metadata
     */
    public ConnectorTableDetailMetadata(final SchemaTableName table,
                                        final List<ColumnMetadata> columns,
                                        final Map<String, String> metadata) {
        this(table, columns, null, null, metadata, null);
    }

    /**
     * Constructor.
     *
     * @param table       table name
     * @param columns     list of columns
     * @param storageInfo storage info
     * @param metadata    metadata
     */
    public ConnectorTableDetailMetadata(final SchemaTableName table,
                                        final List<ColumnMetadata> columns,
                                        final StorageInfo storageInfo,
                                        final Map<String, String> metadata) {
        this(table, columns, null, storageInfo, metadata, null);
    }

    /**
     * Constructor.
     *
     * @param table       table name
     * @param columns     list of columns
     * @param owner       owner name
     * @param storageInfo storage info
     * @param metadata    metadata
     * @param auditInfo   audit info
     */
    public ConnectorTableDetailMetadata(final SchemaTableName table, final List<ColumnMetadata> columns,
                                        final String owner,
                                        final StorageInfo storageInfo, final Map<String, String> metadata,
                                        final AuditInfo auditInfo) {
        super(table, columns, Maps.newHashMap(), owner, false);
        this.storageInfo = storageInfo;
        this.metadata = metadata;
        this.auditInfo = auditInfo != null ? auditInfo : new AuditInfo();
    }
}
