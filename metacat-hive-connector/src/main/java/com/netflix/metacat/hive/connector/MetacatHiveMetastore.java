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
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

/**
 * Created by amajumdar on 4/20/15.
 */
public interface MetacatHiveMetastore extends HiveMetastore {
    /**
     * Create schema/database
     * @param database database metadata
     */
    void createDatabase(Database database) throws AlreadyExistsException;

    /**
     * Update schema/database
     * @param database database metadata
     */
    void updateDatabase(Database database) throws NoSuchObjectException;

    /**
     * Drop database
     * @param dbName database name
     */
    void dropDatabase(String dbName) throws NoSuchObjectException;

    /**
     * Alter the given table
     * @param table the table name
     */
    void alterTable(final Table table) throws NoSuchObjectException;

    /**
     * Returns the list of tables
     * @param dbName database name
     * @param tableNames list of table names
     * @return list of tables
     */
    List<Table> getTablesByNames(String dbName, List<String> tableNames);

    /**
     * Get partitions for the given database and table name using the filter expression
     * @param dbName database name
     * @param tableName table name
     * @param filter filter expression (JSP comparable expression)
     * @return list of partitions
     */
    List<Partition> getPartitions(String dbName, String tableName, String filter) throws NoSuchObjectException;

    /**
     * Get partitions for the list of partition names under the given database and table name.
     * @param dbName database name
     * @param tableName table name
     * @param partitionIds partition ids/names
     * @return list of partitions
     */
    List<Partition> getPartitions(String dbName, String tableName, List<String> partitionIds) throws NoSuchObjectException;

    /**
     * Saves the partitions.
     * @param partitions list of partitions
     */
    void savePartitions(List<Partition> partitions) throws NoSuchObjectException, AlreadyExistsException;

    /**
     * Saves the partitions. Deletes the partitions with the given <code>delPartitionNames</code>
     * @param partitions list of partitions
     */
    void addDropPartitions(String dbName, String tableName, List<Partition> partitions, List<String> delPartitionNames) throws NoSuchObjectException, AlreadyExistsException;

    /**
     * Drops the partition for the given database, table and partition name.
     * @param dbName database name
     * @param tableName table name
     * @param partitionNames partition ids/names
     */
    void dropPartitions( String dbName, String tableName, List<String> partitionNames) throws NoSuchObjectException;
}
