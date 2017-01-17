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


import com.google.common.collect.Lists;

import java.util.List;

/**
 * Extension of ConnectorMetadata.
 * @author zhenl
 */
public interface ConnectorDetailMetadata extends ConnectorMetadata {
    /**
     * Creates a schema with the given <code>schemaName</code>.
     * @param session connector session
     * @param schema schema metadata
     */
    void createSchema(ConnectorSession session, ConnectorSchemaMetadata schema);

    /**
     * Updates a schema with the given <code>schemaName</code>.
     * @param session connector session
     * @param schema schema metadata
     */
    void updateSchema(ConnectorSession session, ConnectorSchemaMetadata schema);

    /**
     * Drop a schema with the given <code>schemaName</code>.
     * @param session connector session
     * @param schemaName schema name
     */
    void dropSchema(ConnectorSession session, String schemaName);

    /**
     * Return schema with the given <code>schemaName</code>.
     * @param session connector session
     * @param schemaName schema name
     * @return ConnectorSchemaMetadata
     */
    ConnectorSchemaMetadata getSchema(ConnectorSession session, String schemaName);

    /**
     * Updates a table using the specified table metadata.
     * @param session connector session
     * @param tableMetadata table details
     * @return table handle
     */
    ConnectorTableHandle alterTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    /**
     * Returns all the table names referring to the given <code>uri</code>.
     * @param uri location
     * @param prefixSearch if tru, we look for tables whose location starts with the given <code>uri</code>
     * @return list of table names
     */
    default List<SchemaTableName> getTableNames(String uri, boolean prefixSearch) {
        return Lists.newArrayList();
    }

    /**
     * Similar to listTables but this method will return the list of tables along with its metadata.
     * @param session connector session
     * @param schemaName schema name
     * @param tableNames table names
     * @return list of table metadata
     */
    List<ConnectorTableMetadata> listTableMetadatas(ConnectorSession session, String schemaName,
                                                    List<String> tableNames);
}
