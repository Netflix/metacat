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
package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.QualifiedName;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Table Info.
 *
 * @author amajumdar
 * @since 1.0.0
 */
@SuppressWarnings("checkstyle:finalclass")
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class TableInfo extends BaseInfo {
    private List<FieldInfo> fields;
    private StorageInfo serde;
    private ViewInfo view;
    private KeySetInfo keys;

    private KeySetInfo getKeys()
    {
        if (this.keys != null) {
            return keys;
        }
        return KeySetInfo.buildKeySet(this.fields);
    }

    /**
     * Constructor.
     *
     * @param name      name of the table
     * @param auditInfo audit information of the table
     * @param metadata  metadata of the table.
     * @param fields    list of columns
     * @param serde     storage informations
     */
    @Builder
    private TableInfo(
        final QualifiedName name,
        final AuditInfo auditInfo,
        final Map<String, String> metadata,
        final List<FieldInfo> fields,
        final StorageInfo serde,
        final ViewInfo view,
        final KeySetInfo keys
    ) {
        super(name, auditInfo, metadata);
        this.fields = fields;
        this.serde = serde;
        this.view = view;
        this.keys = keys;
    }
}
