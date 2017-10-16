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

import com.netflix.metacat.connector.hive.client.embedded.HivePrivilege;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * IMetacatHiveClient.
 *
 * @author zhenl
 * @since 1.0.0
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
    default void createDatabase(final Database database) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Drop database.
     *
     * @param dbName database name
     * @throws TException NotfoundException
     */
    default void dropDatabase(final String dbName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @return database database
     * @throws TException NotfoundException
     */
    default Database getDatabase(final String databaseName) throws TException {
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
    default List<String> getAllTables(final String databaseName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return table information
     * @throws TException NotfoundException
     */
    default Table getTableByName(final String databaseName,
                                 final String tableName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Create table.
     *
     * @param table database metadata
     * @throws TException already exist TException
     */
    default void createTable(final Table table) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Delete table.
     *
     * @param databaseName database
     * @param tableName    tableName
     * @throws TException NotfoundException
     */
    default void dropTable(final String databaseName,
                           final String tableName) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Rename table.
     *
     * @param databaseName    database
     * @param oldTableName    tablename
     * @param newdatabadeName newdatabase
     * @param newTableName    newName
     * @throws TException NotfoundException
     */
    default void rename(final String databaseName,
                        final String oldTableName,
                        final String newdatabadeName,
                        final String newTableName) throws TException {
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
    default void alterTable(final String databaseName,
                            final String tableName,
                            final Table table) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Update table.
     *
     * @param databaseName databaseName
     * @param database     table
     * @throws TException if the database does not exist
     */
    default void alterDatabase(final String databaseName,
                               final Database database) throws TException {
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
    default List<Partition> getPartitions(final String databaseName,
                                          final String tableName,
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
    default void dropPartitions(final String databaseName,
                                final String tableName,
                                final List<String> partitionNames) throws
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
    default List<Partition> listPartitionsByFilter(final String databaseName,
                                                   final String tableName,
                                                   final String filter
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
    default int getPartitionCount(final String databaseName,
                                  final String tableName) throws TException {

        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Get partition keys.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return list of partition names
     * @throws TException TException
     */
    default List<String> getPartitionNames(final String databaseName,
                                           final String tableName)
        throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Save partitions.
     *
     * @param partitions partitions
     * @throws TException TException
     */
    default void savePartitions(final List<Partition> partitions)
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
    default void alterPartitions(final String dbName, final String tableName,
                                 final List<Partition> partitions) throws
        TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * addDropPartitions.
     *
     * @param dbName            dbName
     * @param tableName         tableName
     * @param partitions        partittions
     * @param delPartitionNames deletePartitionNames
     * @throws TException TException
     */
    default void addDropPartitions(final String dbName, final String tableName,
                                   final List<Partition> partitions,
                                   final List<String> delPartitionNames) throws TException {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }


    /**
     * getDatabasePrivileges.
     *
     * @param user         user
     * @param databaseName databaseName
     * @return set of privilege
     */
    default Set<HivePrivilege> getDatabasePrivileges(String user, String databaseName) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * getTablePrivileges.
     *
     * @param user      user
     * @param tableName databaseName
     * @return set of privilege
     */
    default Set<HivePrivilege> getTablePrivileges(String user, String tableName) {
        throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
    }

    /**
     * Clean up any held resources.
     *
     * @throws TException TException
     */
    default void shutdown() throws TException {
    }
}
