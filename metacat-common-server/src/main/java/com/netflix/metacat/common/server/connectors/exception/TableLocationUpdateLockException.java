/*
 *
 *  Copyright 2024 Netflix, Inc.
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
package com.netflix.metacat.common.server.connectors.exception;

import com.netflix.metacat.common.QualifiedName;

/**
 * Exception thrown when a table update is blocked because location updates are locked for the catalog.
 *
 * @author jursetta
 */
public class TableLocationUpdateLockException extends ConnectorException {

    /**
     * Constructor.
     *
     * @param catalogName the catalog where location updates are locked
     * @param tableName   the table being updated
     */
    public TableLocationUpdateLockException(final String catalogName, final QualifiedName tableName) {
        super(String.format("Table location update is locked for catalog '%s' on table '%s'",
            catalogName, tableName));
    }
}
