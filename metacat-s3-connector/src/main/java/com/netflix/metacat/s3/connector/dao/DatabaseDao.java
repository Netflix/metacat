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

import com.netflix.metacat.connector.s3.model.Database;

import java.util.List;

/**
 * Database DAO.
 */
public interface DatabaseDao extends BaseDao<Database> {
    /**
     * Get database for the given source and database name.
     * @param sourceName source name
     * @param databaseName database name
     * @return Database
     */
    Database getBySourceDatabaseName(String sourceName, String databaseName);

    /**
     * Get list of databases for the given source name and database names.
     * @param sourceName source name
     * @param databaseNames list of database names
     * @return list of databases
     */
    List<Database> getBySourceDatabaseNames(String sourceName, List<String> databaseNames);
}
