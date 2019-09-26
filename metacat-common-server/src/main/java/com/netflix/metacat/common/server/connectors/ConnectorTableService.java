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

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.model.TableInfo;

import java.util.List;
import java.util.Map;

/**
 * Service interface for connector to implement and expose Table related metadata.
 *
 * @author tgianos
 * @since 1.0.0
 */
public interface ConnectorTableService extends ConnectorBaseService<TableInfo> {

    /**
     * Returns all the table names referring to the given <code>uris</code>.
     *
     * @param context      The Metacat request context
     * @param uris         locations
     * @param prefixSearch if true, we look for tables whose location starts with the given <code>uri</code>
     * @return map of uri to list of partition names
     * @throws UnsupportedOperationException If the connector doesn't implement this method
     */
    default Map<String, List<QualifiedName>> getTableNames(
        final ConnectorRequestContext context,
        final List<String> uris,
        final boolean prefixSearch
    ) {
        throw new UnsupportedOperationException(ConnectorBaseService.UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns a filtered list of table names.
     * @param context   The Metacat request context
     * @param filter    filter expression
     * @param name      qualified name of either the catalog or database
     * @param limit     size of the list
     * @return list of table names
     */
    default List<QualifiedName> getTableNames(
        final ConnectorRequestContext context,
        final QualifiedName name,
        final String filter,
        final Integer limit) {
        throw new UnsupportedOperationException(ConnectorBaseService.UNSUPPORTED_MESSAGE);
    }
}
