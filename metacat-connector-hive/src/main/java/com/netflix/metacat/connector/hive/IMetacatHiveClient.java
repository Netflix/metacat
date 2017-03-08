/*
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
 */

package com.netflix.metacat.connector.hive;

import lombok.NonNull;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * IMetacatHiveClient.
 *
 * @author zhenl
 */
public interface IMetacatHiveClient {
    /**
     * Standard error message for all default implementations.
     */
    String UNSUPPORTED_MESSAGE = "Not supported for this client";

    /**
     * Create database.
     *
     * @param database database metadata
     * @throws Exception already exist exception
     */
    default void createDatabase(@NonNull final Database database) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Drop database.
     *
     * @param dbName database name
     * @throws Exception NotfoundException
     */
    default void dropDatabase(@NonNull final String dbName) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @return database database
     * @throws Exception NotfoundException
     */
    default Database getDatabase(@Nonnull final String databaseName) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * List all databases.
     *
     * @return database list
     * @throws Exception exceptions
     */
    default List<String> getAllDatabases() throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Get all tables.
     *
     * @param databaseName databasename
     * @return tableNames
     * @throws Exception metaexception
     */
    default List<String> getAllTables(@Nonnull final String databaseName) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return list of tables
     * @throws Exception NotfoundException
     */
    default Table getTableByName(@Nonnull final String databaseName,
                                 @NonNull final String tableName) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Create table.
     *
     * @param table database metadata
     * @throws Exception already exist exception
     */
    default void createTable(@NonNull final Table table) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Delete table.
     *
     * @param databaseName database
     * @param tableName    tableName
     * @throws Exception NotfoundException
     */
    default void dropTable(@Nonnull final String databaseName, @NonNull final String tableName) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Rename table.
     *
     * @param databaseName    database
     * @param oldName         tablename
     * @param newdatabadeName newdatabase
     * @param newName         newName
     * @throws Exception NotfoundException
     */
    default void rename(@Nonnull final String databaseName,
                        @NonNull final String oldName,
                        @Nonnull final String newdatabadeName,
                        @Nonnull final String newName) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Update table.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @param table        table
     * @throws Exception if the database does not exist
     */
    default void alterTable(@NonNull final String databaseName,
                            @NonNull final String tableName,
                            @NonNull final Table table) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table.
     *
     * @param databaseName   databaseName
     * @param tableName      tableName
     * @param partitionNames partitionName
     * @return list of partitions
     * @throws Exception Exception
     */
    default List<Partition> getPartitions(@Nonnull final String databaseName,
                                          @NonNull final String tableName,
                                          @Nullable final List<String> partitionNames) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Drop a list of partitions.
     *
     * @param databaseName   databaseName
     * @param tableName      tableName
     * @param partitionNames partitionNames
     * @throws Exception TException
     */
    default void dropPartitions(@Nonnull final String databaseName,
                                @NonNull final String tableName,
                                @Nonnull final List<String> partitionNames) throws
            Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * List partitions.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @param filter       filter
     * @return List of partitions
     * @throws Exception exception
     */
    default List<Partition> listPartitionsByFilter(@Nonnull final String databaseName,
                                                   @NonNull final String tableName,
                                                   @Nonnull final String filter
    ) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Get partition count.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return partition count
     * @throws Exception exception
     */
    default int getPartitionCount(@Nonnull final String databaseName,
                                  @NonNull final String tableName) throws Exception {

        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Get partition keys.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return List<String> partitionNames
     * @throws Exception exception
     */
    default List<String> getPartitionNames(@Nonnull final String databaseName,
                                           @NonNull final String tableName)
            throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Save partitions.
     *
     * @param partitions partitions
     * @throws Exception exception
     */
    default void savePartitions(@Nonnull final List<Partition> partitions)
            throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Alter partitions.
     *
     * @param dbName     databaseName
     * @param tableName  tableName
     * @param partitions partitions
     * @throws Exception exception
     */
    default void alterPartitions(@Nonnull final String dbName, @Nonnull final String tableName,
                                 @Nonnull final List<Partition> partitions) throws
            Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * addDropPartitions.
     *
     * @param dbName dbName
     * @param tableName tableName
     * @param partitions partittions
     * @param delPartitionNames deletePartitionNames
     * @throws Exception exception
     */
    default void addDropPartitions(final String dbName, final String tableName,
                                   final List<Partition> partitions,
                                   final List<String> delPartitionNames) throws Exception {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }
}
