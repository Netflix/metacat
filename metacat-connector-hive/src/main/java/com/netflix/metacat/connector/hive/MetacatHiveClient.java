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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.List;

/**
 * MetacatHiveClient.
 *
 * @author zhenl
 */
public class MetacatHiveClient {
    private static final short ALL_RESULTS = -1;
    private HiveConf hiveConf;


    /**
     * Constructor.
     *
     * @param hiveConf hiveConf
     * @throws MetaException exception
     */
    @Inject
    public MetacatHiveClient(final HiveConf hiveConf) throws MetaException {
        this.hiveConf = hiveConf;
    }

    /**
     * Create a metastore client instance.
     *
     * @return hivemetastore client
     * @throws MetaException metaexception
     */
    HiveMetaStoreClient createMetastoreClient() throws MetaException {
        return new HiveMetaStoreClient(hiveConf);
    }

    /**
     * List all databases.
     *
     * @return database list
     */
    public List<String> getAllDatabases() throws MetaException {
        return createMetastoreClient().getAllDatabases();
    }

    /**
     * Get all tables.
     *
     * @param databaseName
     * @return table names
     * @throws MetaException
     */
    public List<String> getAllTables(@Nonnull final String databaseName) throws MetaException {
        return createMetastoreClient().getAllTables(databaseName);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return list of tables
     * @throws TException NotfoundException
     */
    Table getTableByName(@Nonnull final String databaseName, @NonNull final String tableName) throws TException {
        return createMetastoreClient().getTable(databaseName, tableName);
    }

    /**
     * Create table.
     *
     * @param table database metadata
     * @throws TException already exist exception
     */
    void createTable(@NonNull final Table table) throws TException {
        createMetastoreClient().createTable(table);
    }

    /**
     * Delete table.
     *
     * @param databaseName database
     * @param tableName    tableName
     * @throws TException NotfoundException
     */
    void dropTable(@Nonnull final String databaseName, @NonNull final String tableName) throws TException {
        createMetastoreClient().dropTable(databaseName, tableName);
    }

    /**
     * Update table.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @param table        table
     * @throws NoSuchObjectException if the database does not exist
     */
    void alterTable(@NonNull final String databaseName,
                    @NonNull final String tableName,
                    @NonNull final Table table) throws TException {
        createMetastoreClient().alter_table(databaseName, tableName, table);
    }

    /**
     * Create database.
     *
     * @param database database metadata
     * @throws TException already exist exception
     */
    void createDatabase(@NonNull final Database database) throws TException {
        createMetastoreClient().createDatabase(database);
    }

    /**
     * Drop database.
     *
     * @param dbName database name
     * @throws TException NotfoundException
     */
    public void dropDatabase(@NonNull final String dbName) throws TException {
        createMetastoreClient().dropDatabase(dbName);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @return database database
     * @throws TException NotfoundException
     */
    Database getDatabase(@Nonnull final String databaseName) throws TException {
        return createMetastoreClient().getDatabase(databaseName);
    }

    /**
     * Returns the table.
     *
     * @param databaseName  databaseName
     * @param tableName     tableName
     * @param partitionName partitionName
     * @return list of partitions
     * @throws TException NotfoundException
     */
    Partition getPartition(@Nonnull final String databaseName,
                           @NonNull final String tableName,
                           @Nonnull final String partitionName) throws NoSuchObjectException, MetaException,
        TException {
        return createMetastoreClient().getPartition(databaseName, tableName, partitionName);
    }

    /**
     * Drop a list of partitions.
     *
     * @param databaseName   databaseName
     * @param tableName      tableName
     * @param partitionNames partitionNames
     * @throws NoSuchObjectException NoSuchObjectException
     * @throws MetaException         MetaException
     * @throws TException            TException
     */
    void dropPartition(@Nonnull final String databaseName,
                       @NonNull final String tableName,
                       @Nonnull final List<String> partitionNames) throws NoSuchObjectException, MetaException,
        TException {
        if (partitionNames != null && !partitionNames.isEmpty()) {
            createMetastoreClient().dropPartition(databaseName, tableName, partitionNames);
        }
    }

    /**
     * List partitions.
     *
     * @param databaseName   databaseName
     * @param tableName      tableName
     * @param partitionNames partitionNames
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     */
    List<Partition> listPartitions(@Nonnull final String databaseName,
                                   @NonNull final String tableName,
                                   @Nonnull final List<String> partitionNames
    ) throws NoSuchObjectException, MetaException,
        TException {
        return createMetastoreClient().listPartitions(databaseName, tableName, partitionNames, ALL_RESULTS);
    }

    /**
     * List all partitions.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     */
    List<Partition> listAllPartitions(@Nonnull final String databaseName,
                                      @NonNull final String tableName
    ) throws NoSuchObjectException, MetaException,
        TException {
        return createMetastoreClient().listPartitions(databaseName, tableName, ALL_RESULTS);
    }

    /**
     * List partitions.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @param filter       filter
     * @return List of partitions
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     */
    List<Partition> listPartitionsByFilter(@Nonnull final String databaseName,
                                           @NonNull final String tableName,
                                           @Nonnull final String filter
    ) throws NoSuchObjectException, MetaException, TException {
        return createMetastoreClient().listPartitionsByFilter(databaseName, tableName, filter, ALL_RESULTS);
    }

    /**
     * Get partition count.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return partition count
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     */
    int getPartitionCount(@Nonnull final String databaseName,
                          @NonNull final String tableName) throws NoSuchObjectException, MetaException, TException {

        return createMetastoreClient().listPartitionNames(databaseName, tableName, ALL_RESULTS).size();
    }

    /**
     * Get partition keys.
     *
     * @param databaseName
     * @param tableName
     * @return
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     */
    List<String> getPartitionNames(@Nonnull final String databaseName,
                                   @NonNull final String tableName)
        throws NoSuchObjectException, MetaException, TException {
        return createMetastoreClient().listPartitionNames(databaseName, tableName, ALL_RESULTS);
    }

    /**
     * Save partitions.
     *
     * @param partitions
     * @throws NoSuchObjectException
     * @throws MetaException
     * @throws TException
     */
    void savePartitions(@Nonnull final List<Partition> partitions)
        throws NoSuchObjectException, MetaException, TException {
        createMetastoreClient().add_partitions(partitions);
    }
}
