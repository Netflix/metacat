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

package com.netflix.metacat.connector.hive.metastoreclient.thrift;

import com.google.common.base.Preconditions;
import com.netflix.metacat.common.server.exception.ConnectorException;
import com.netflix.metacat.common.server.exception.InvalidMetaException;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
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
public class MetacatHiveClient implements IMetacatHiveClient {
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
    private HiveMetastoreClient createMetastoreClient() {
        try {
            return hiveMetastoreClientFactory.create(host, port);
        } catch (TTransportException e) {
            throw new RuntimeException("Failed connecting to Hive metastore: " + host + ":" + port, e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllDatabases() throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            return client.get_all_databases();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllTables(@Nonnull final String databaseName) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            return client.get_all_tables(databaseName);
        }
    }


    /**
     * {@inheritDoc}.
     */
    @Override
    public Table getTableByName(@Nonnull final String databaseName,
                                @NonNull final String tableName) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            return client.get_table(databaseName, tableName);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createTable(@NonNull final Table table) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            client.create_table(table);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropTable(@Nonnull final String databaseName, @NonNull final String tableName) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            client.drop_table(databaseName, tableName, false);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void rename(@Nonnull final String databaseName,
                       @NonNull final String oldName,
                       @Nonnull final String newdatabadeName,
                       @Nonnull final String newName) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            final Table table = client.get_table(databaseName, oldName);
            client.drop_table(databaseName, oldName, false);
            table.setDbName(newdatabadeName);
            table.setTableName(newName);
            client.create_table(table);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterTable(@NonNull final String databaseName,
                           @NonNull final String tableName,
                           @NonNull final Table table) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            client.alter_table(databaseName, tableName, table);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createDatabase(@NonNull final Database database) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            client.create_database(database);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropDatabase(@NonNull final String dbName) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            client.drop_database(dbName, false, false);
        }
    }


    /**
     * {@inheritDoc}.
     */
    @Override
    public Database getDatabase(@Nonnull final String databaseName) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            return client.get_database(databaseName);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<Partition> getPartitions(@Nonnull final String databaseName,
                                         @NonNull final String tableName,
                                         @Nullable final List<String> partitionNames) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            if (partitionNames != null && !partitionNames.isEmpty()) {
                return client.get_partitions_by_names(databaseName, tableName, partitionNames);
            } else {
                return client.get_partitions(databaseName, tableName, ALL_RESULTS);
            }
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropPartitions(@Nonnull final String databaseName,
                               @NonNull final String tableName,
                               @Nonnull final List<String> partitionNames) throws
            TException {
        dropHivePartitions(createMetastoreClient(), databaseName, tableName, partitionNames);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<Partition> listPartitionsByFilter(@Nonnull final String databaseName,
                                                  @NonNull final String tableName,
                                                  @Nonnull final String filter
    ) throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            return client.get_partitions_by_filter(databaseName, tableName, filter, ALL_RESULTS);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int getPartitionCount(@Nonnull final String databaseName,
                                 @NonNull final String tableName) throws TException {

        return getPartitions(databaseName, tableName, null).size();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionNames(@Nonnull final String databaseName,
                                          @NonNull final String tableName)
            throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            return client.get_partition_names(databaseName, tableName, ALL_RESULTS);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void savePartitions(@Nonnull final List<Partition> partitions)
            throws TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            client.add_partitions(partitions);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterPartitions(@Nonnull final String dbName, @Nonnull final String tableName,
                                @Nonnull final List<Partition> partitions) throws
            TException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            client.alter_partitions(dbName, tableName, partitions);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void addDropPartitions(final String dbName, final String tableName,
                           final List<Partition> partitions,
                           final List<String> delPartitionNames) throws NoSuchObjectException {
        try (HiveMetastoreClient client = createMetastoreClient()) {
            try {
                dropHivePartitions(client, dbName, tableName, delPartitionNames);
                client.add_partitions(partitions);
            } catch (MetaException | InvalidObjectException e) {
                throw new InvalidMetaException("One or more partitions are invalid.", e);
            } catch (TException e) {
                throw new ConnectorException(
                    String.format("Internal server error adding/dropping partitions for table %s.%s",
                        dbName, tableName), e);
            }
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
