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

package com.netflix.metacat.connector.s3.dao;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.connector.s3.model.Table;

import java.util.List;
import java.util.Map;

/**
 * Table DAO.
 */
public interface TableDao extends BaseDao<Table> {
    /**
     * Get a table.
     * @param sourceName source name
     * @param databaseName database name
     * @param tableName table name
     * @return table
     */
    Table getBySourceDatabaseTableName(String sourceName, String databaseName, String tableName);

    /**
     * Get list of tables.
     * @param sourceName source name
     * @param databaseName database name
     * @param tableNames table names
     * @return tables
     */
    List<Table> getBySourceDatabaseTableNames(String sourceName, String databaseName, List<String> tableNames);

    /**
     * Get list of databases for the given source name and database name prefix.
     * @param sourceName source name
     * @param databaseName database name
     * @param tableNamePrefix table name prefix
     * @param sort sort
     * @param pageable pageable
     * @return list of tables
     */
    List<Table> searchBySourceDatabaseTableName(String sourceName, String databaseName, String tableNamePrefix,
        Sort sort, Pageable pageable);

    /**
     * Gets the names of the tables for the given uris.
     * @param sourceName source name
     * @param uris list of uri paths
     * @param prefixSearch if true, will do a prefix search
     * @return Map of uri to list of table names
     */
    Map<String, List<QualifiedName>> getByUris(String sourceName, List<String> uris, boolean prefixSearch);
}
