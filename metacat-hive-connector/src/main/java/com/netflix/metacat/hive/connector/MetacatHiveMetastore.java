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

package com.netflix.metacat.hive.connector;

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.NotFoundException;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

/**
 * Metacat Hive metastore.
 */
public interface MetacatHiveMetastore extends HiveMetastore {
    /**
     * Create schema/database.
     * @param database database metadata
     * @throws AlreadyExistsException if database with the same name exists
     */
    void createDatabase(Database database) throws AlreadyExistsException;

    /**
     * Update schema/database.
     * @param database database metadata
     * @throws NoSuchObjectException if the database does not exist
     */
    void updateDatabase(Database database) throws NoSuchObjectException;

    /**
     * Drop database.
     * @param dbName database name
     * @throws NoSuchObjectException if the database does not exist
     */
    void dropDatabase(String dbName) throws NoSuchObjectException;

    /**
     * Alter the given table.
     * @param table the table name
     * @throws NoSuchObjectException if the table does not exist
     */
    void alterTable(Table table) throws NoSuchObjectException;

    /**
     * Returns the list of tables.
     * @param dbName database name
     * @param tableNames list of table names
     * @return list of tables
     */
    List<Table> getTablesByNames(String dbName, List<String> tableNames);

    /**
     * Get partitions for the given database and table name using the filter expression.
     * @param dbName database name
     * @param tableName table name
     * @param filter filter expression (JSP comparable expression)
     * @return list of partitions
     * @throws NotFoundException if the table does not exist
     */
    List<Partition> getPartitions(String dbName, String tableName, String filter) throws NotFoundException;

    /**
     * Get partitions for the list of partition names under the given database and table name.
     * @param dbName database name
     * @param tableName table name
     * @param partitionIds partition ids/names
     * @return list of partitions
     * @throws NotFoundException if the table does not exist
     */
    List<Partition> getPartitions(String dbName, String tableName, List<String> partitionIds) throws NotFoundException;

    /**
     * Saves partitions.
     * @param partitions list of partitions
     * @throws NoSuchObjectException if the table does not exist
     * @throws AlreadyExistsException if the partition already exist
     */
    void savePartitions(List<Partition> partitions) throws NoSuchObjectException, AlreadyExistsException;

    /**
     * Alter partitions.
     * @param dbName database name
     * @param tableName table name
     * @param partitions list of partitions
     * @throws NoSuchObjectException if the table does not exist
     */
    void alterPartitions(String dbName, String tableName, List<Partition> partitions) throws NoSuchObjectException;

    /**
     * Saves the partitions. Deletes the partitions with the given <code>delPartitionNames</code>
     * @param dbName database name
     * @param tableName table name
     * @param partitions list of partitions
     * @param delPartitionNames list of names of the partitions to be deleted
     * @throws NoSuchObjectException if the table does not exist
     * @throws AlreadyExistsException if the partition already exist
     */
    void addDropPartitions(String dbName, String tableName, List<Partition> partitions, List<String> delPartitionNames)
        throws NoSuchObjectException, AlreadyExistsException;

    /**
     * Drops the partition for the given database, table and partition name.
     * @param dbName database name
     * @param tableName table name
     * @param partitionNames partition ids/names
     * @throws NoSuchObjectException if the partition does not exist
     */
    void dropPartitions(String dbName, String tableName, List<String> partitionNames) throws NoSuchObjectException;
}
