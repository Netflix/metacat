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

import com.google.common.base.Preconditions;
import com.netflix.metacat.common.server.exception.ConnectorException;
import com.netflix.metacat.common.server.exception.InvalidMetaException;
import lombok.NonNull;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.net.URI;
import java.util.List;

/**
 * MetacatHiveClient.
 *
 * @author zhenl
 */
public class MetacatHiveClient {
    private static final short ALL_RESULTS = -1;
    private HiveMetastoreClientFactory hiveMetastoreClientFactory;
    private final String host;
    private final int port;

    /**
     * Constructor.
     *
     * @param address                    address
     * @param hiveMetastoreClientFactory hiveMetastoreClientFactory
     * @throws MetaException exception
     */
    @Inject
    public MetacatHiveClient(@Named("thrifturi") @Nonnull final URI address,
                             @Nonnull final HiveMetastoreClientFactory hiveMetastoreClientFactory)
            throws MetaException {
        this.hiveMetastoreClientFactory = hiveMetastoreClientFactory;
        Preconditions.checkArgument(address.getHost() != null, "metastoreUri host is missing: " + address);
        Preconditions.checkArgument(address.getPort() != -1, "metastoreUri port is missing: " + address);
        this.host = address.getHost();
        this.port = address.getPort();
    }

    /**
     * Create a metastore client instance.
     *
     * @return hivemetastore client
     */
    HiveMetastoreClient createMetastoreClient() {
        try {
            return hiveMetastoreClientFactory.create(host, port);
        } catch (TTransportException e) {
            throw new RuntimeException("Failed connecting to Hive metastore: " + host + ":" + port, e);
        }
    }

    /**
     * List all databases.
     *
     * @return database list
     * @throws TException exceptions
     */
    public List<String> getAllDatabases() throws TException {
        return createMetastoreClient().get_all_databases();
    }

    /**
     * Get all tables.
     *
     * @param databaseName databasename
     * @return tableNames
     * @throws TException metaexception
     */
    public List<String> getAllTables(@Nonnull final String databaseName) throws TException {
        return createMetastoreClient().get_all_tables(databaseName);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return list of tables
     * @throws TException NotfoundException
     */
    Table getTableByName(@Nonnull final String databaseName,
                         @NonNull final String tableName) throws TException {
        return createMetastoreClient().get_table(databaseName, tableName);
    }

    /**
     * Create table.
     *
     * @param table database metadata
     * @throws TException already exist exception
     */
    void createTable(@NonNull final Table table) throws TException {
        createMetastoreClient().create_table(table);
    }

    /**
     * Delete table.
     *
     * @param databaseName database
     * @param tableName    tableName
     * @throws TException NotfoundException
     */
    void dropTable(@Nonnull final String databaseName, @NonNull final String tableName) throws TException {
        createMetastoreClient().drop_table(databaseName, tableName, false);
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
    public void rename(@Nonnull final String databaseName,
                       @NonNull final String oldName,
                       @Nonnull final String newdatabadeName,
                       @Nonnull final String newName) throws TException {
        final HiveMetastoreClient hiveMetaStoreClient = createMetastoreClient();
        final Table table = hiveMetaStoreClient.get_table(databaseName, oldName);
        hiveMetaStoreClient.drop_table(databaseName, oldName, false);
        table.setDbName(newdatabadeName);
        table.setTableName(newName);
        hiveMetaStoreClient.create_table(table);
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
        createMetastoreClient().create_database(database);
    }

    /**
     * Drop database.
     *
     * @param dbName database name
     * @throws TException NotfoundException
     */
    public void dropDatabase(@NonNull final String dbName) throws TException {
        createMetastoreClient().drop_database(dbName, false, false);
    }

    /**
     * Returns the table.
     *
     * @param databaseName databaseName
     * @return database database
     * @throws TException NotfoundException
     */
    Database getDatabase(@Nonnull final String databaseName) throws TException {
        return createMetastoreClient().get_database(databaseName);
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
    List<Partition> getPartitions(@Nonnull final String databaseName,
                                  @NonNull final String tableName,
                                  @Nullable final List<String> partitionNames) throws TException {
        final HiveMetastoreClient client = createMetastoreClient();
        if (partitionNames != null && !partitionNames.isEmpty()) {
            return client.get_partitions_by_names(databaseName, tableName, partitionNames);
        } else {
            return client.get_partitions(databaseName, tableName, ALL_RESULTS);
        }
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
    void dropPartitions(@Nonnull final String databaseName,
                        @NonNull final String tableName,
                        @Nonnull final List<String> partitionNames) throws
            TException {
        dropHivePartitions(createMetastoreClient(), databaseName, tableName, partitionNames);
    }

    /**
     * List partitions.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @param filter       filter
     * @return List of partitions
     * @throws TException
     */
    List<Partition> listPartitionsByFilter(@Nonnull final String databaseName,
                                           @NonNull final String tableName,
                                           @Nonnull final String filter
    ) throws TException {
        return createMetastoreClient().get_partitions_by_filter(databaseName, tableName, filter, ALL_RESULTS);
    }

    /**
     * Get partition count.
     *
     * @param databaseName databaseName
     * @param tableName    tableName
     * @return partition count
     * @throws TException
     */
    int getPartitionCount(@Nonnull final String databaseName,
                          @NonNull final String tableName) throws TException {

        return getPartitions(databaseName, tableName, null).size();
    }

    /**
     * Get partition keys.
     *
     * @param databaseName
     * @param tableName
     * @return
     * @throws TException
     */
    List<String> getPartitionNames(@Nonnull final String databaseName,
                                   @NonNull final String tableName)
            throws TException {
        return createMetastoreClient().get_partition_names(databaseName, tableName, ALL_RESULTS);
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
            throws TException {
        createMetastoreClient().add_partitions(partitions);
    }

    /**
     * Alter partitions.
     *
     * @param dbName
     * @param tableName
     * @param partitions
     * @throws TException
     */
    void alterPartitions(@Nonnull final String dbName, @Nonnull final String tableName,
                         @Nonnull final List<Partition> partitions) throws
            TException {
        createMetastoreClient().alter_partitions(dbName, tableName, partitions);
    }

    void addDropPartitions(final String dbName, final String tableName,
                           final List<Partition> partitions,
                           final List<String> delPartitionNames) throws NoSuchObjectException {
        try {
            dropHivePartitions(createMetastoreClient(), dbName, tableName, delPartitionNames);
            createMetastoreClient().add_partitions(partitions);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (TException e) {
            throw new ConnectorException(
                    String.format("Internal server error adding/dropping partitions for table %s.%s",
                            dbName, tableName), e);
        }
    }


    private void dropHivePartitions(final HiveMetastoreClient client, final String dbName, final String tableName,
                                    final List<String> partitionNames)
            throws TException {
        if (partitionNames != null && !partitionNames.isEmpty()) {
            final DropPartitionsRequest request = new DropPartitionsRequest(dbName, tableName, new RequestPartsSpec(
                    RequestPartsSpec._Fields.NAMES, partitionNames));
            request.setDeleteData(false);
            client.drop_partitions_req(request);
        }
    }
}
