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
import org.apache.thrift.TException;

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
     * @throws TException already exist TException
     */
    default void createDatabase(@NonNull final Database database) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Drop database.
     *
     * @param dbName database name
     * @throws TException NotfoundException
     */
    default void dropDatabase(@NonNull final String dbName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @return database database
     * @throws TException NotfoundException
     */
    default Database getDatabase(@Nonnull final String databaseName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * List all databases.
     *
     * @return database list
     * @throws TException exceptions
     */
    default List<String> getAllDatabases() throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Get all tables.
     *
     * @param databaseName databasename
     * @return tableNames
     * @throws TException metaexception
     */
    default List<String> getAllTables(@Nonnull final String databaseName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return list of tables
     * @throws TException NotfoundException
     */
    default Table getTableByName(@Nonnull final String databaseName,
                                 @NonNull final String tableName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Create table.
     *
     * @param table database metadata
     * @throws TException already exist TException
     */
    default void createTable(@NonNull final Table table) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Delete table.
     *
     * @param databaseName database
     * @param tableName    tableName
     * @throws TException NotfoundException
     */
    default void dropTable(@Nonnull final String databaseName, @NonNull final String tableName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Rename table.
     *
     * @param databaseName    database
     * @param oldName         tablename
     * @param newdatabadeName newdatabase
     * @param newName         newName
     * @throws TException NotfoundException
     */
    default void rename(@Nonnull final String databaseName,
                        @NonNull final String oldName,
                        @Nonnull final String newdatabadeName,
                        @Nonnull final String newName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Update table.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @param table        table
     * @throws TException if the database does not exist
     */
    default void alterTable(@NonNull final String databaseName,
                            @NonNull final String tableName,
                            @NonNull final Table table) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table.
     *
     * @param databaseName   databaseName
     * @param tableName      tableName
     * @param partitionNames partitionName
     * @return list of partitions
     * @throws TException TException
     */
    default List<Partition> getPartitions(@Nonnull final String databaseName,
                                          @NonNull final String tableName,
                                          @Nullable final List<String> partitionNames) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Drop a list of partitions.
     *
     * @param databaseName   databaseName
     * @param tableName      tableName
     * @param partitionNames partitionNames
     * @throws TException TException
     */
    default void dropPartitions(@Nonnull final String databaseName,
                                @NonNull final String tableName,
                                @Nonnull final List<String> partitionNames) throws
            TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * List partitions.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @param filter       filter
     * @return List of partitions
     * @throws TException TException
     */
    default List<Partition> listPartitionsByFilter(@Nonnull final String databaseName,
                                                   @NonNull final String tableName,
                                                   @Nonnull final String filter
    ) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Get partition count.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return partition count
     * @throws TException TException
     */
    default int getPartitionCount(@Nonnull final String databaseName,
                                  @NonNull final String tableName) throws TException {

        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Get partition keys.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return List<String> partitionNames
     * @throws TException TException
     */
    default List<String> getPartitionNames(@Nonnull final String databaseName,
                                           @NonNull final String tableName)
            throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Save partitions.
     *
     * @param partitions partitions
     * @throws TException TException
     */
    default void savePartitions(@Nonnull final List<Partition> partitions)
            throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Alter partitions.
     *
     * @param dbName     databaseName
     * @param tableName  tableName
     * @param partitions partitions
     * @throws TException TException
     */
    default void alterPartitions(@Nonnull final String dbName, @Nonnull final String tableName,
                                 @Nonnull final List<Partition> partitions) throws
            TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * addDropPartitions.
     *
     * @param dbName dbName
     * @param tableName tableName
     * @param partitions partittions
     * @param delPartitionNames deletePartitionNames
     * @throws TException TException
     */
    default void addDropPartitions(final String dbName, final String tableName,
                                   final List<Partition> partitions,
                                   final List<String> delPartitionNames) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Clean up any held resources.
     * @throws TException TException
     */
    default void shutdown() throws TException { }
}
