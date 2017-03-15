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

package com.netflix.metacat.common.server.connectors.model;

import com.netflix.metacat.common.QualifiedName;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Database information.
 * @since 1.0.0
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public final class DatabaseInfo extends BaseInfo {
    /* location of the database */
    private String uri;

    /**
     * Constructor.
     * @param name name of the database
     * @param auditInfo audit information of the database
     * @param metadata metadata of the database.
     * @param uri location of the database
     */
    @Builder
    private DatabaseInfo(final QualifiedName name, final AuditInfo auditInfo,
                         final Map<String, String> metadata, final String uri) {
        super(name, auditInfo, metadata);
        this.uri = uri;
    }
}
