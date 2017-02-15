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

import com.netflix.metacat.connector.s3.model.Table;

import java.util.List;

/**
 * Table DAO.
 */
public interface TableDao extends BaseDao<Table> {
    /**
     * Get a table.
     * @param sourceName source name
     * @param schemaName schema name
     * @param tableName table name
     * @return table
     */
    Table getBySourceDatabaseTableName(String sourceName, String schemaName, String tableName);

    /**
     * Get list of tables.
     * @param sourceName source name
     * @param schemaName schema name
     * @param tableNames table names
     * @return tables
     */
    List<Table> getBySourceDatabaseTableNames(String sourceName, String schemaName, List<String> tableNames);
}
