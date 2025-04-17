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

package com.netflix.metacat.connector.hive.client.thrift;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nullable;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * MetacatHiveClient.
 *
 * @author zhenl
 * @since 1.0.0
 */
public class MetacatHiveClient implements IMetacatHiveClient {
    private static final short ALL_RESULTS = -1;
    private HiveMetastoreClientFactory hiveMetastoreClientFactory;
    private final String host;
    private final int port;
    private final Registry registry;
    private final Id requestTimerId;
    private final Counter hiveSqlErrorCounter;

    /**
     * Constructor.
     *
     * @param catalogName                catalogName
     * @param address                    address
     * @param hiveMetastoreClientFactory hiveMetastoreClientFactory
     * @param registry                   registry
     * @throws MetaException exception
     */
    public MetacatHiveClient(final String catalogName,
                             final URI address,
                             final HiveMetastoreClientFactory hiveMetastoreClientFactory,
                             final Registry registry
    )
        throws MetaException {
        this.hiveMetastoreClientFactory = hiveMetastoreClientFactory;
        Preconditions.checkArgument(address.getHost() != null, "metastoreUri host is missing: " + address);
        Preconditions.checkArgument(address.getPort() != -1, "metastoreUri port is missing: " + address);
        this.host = address.getHost();
        this.port = address.getPort();
        this.registry = registry;
        this.requestTimerId = registry.createId(HiveMetrics.TimerExternalHiveRequest.getMetricName());
        this.hiveSqlErrorCounter =
            registry.counter(HiveMetrics.CounterHiveSqlLockError.getMetricName() + "." + catalogName);
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
        return callWrap(HiveMetrics.TagGetAllDatabases.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                return client.get_all_databases();
            }
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllTables(final String databaseName) throws TException {
        return callWrap(HiveMetrics.TagGetAllTables.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                return client.get_all_tables(databaseName);
            }
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getTableNames(final String databaseName, final String filter, final int limit)
        throws TException {
        return callWrap(HiveMetrics.TagGetTableNames.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                return client.get_table_names_by_filter(databaseName, filter, (short) limit);
            }
        });
    }


    /**
     * {@inheritDoc}.
     */
    @Override
    public Table getTableByName(final String databaseName,
                                final String tableName) throws TException {
        return callWrap(HiveMetrics.TagGetTableByName.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                return client.get_table(databaseName, tableName);
            }
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createTable(final Table table) throws TException {
        callWrap(HiveMetrics.TagCreateTable.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                client.create_table(table);
            }
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropTable(final String databaseName,
                          final String tableName) throws TException {
        callWrap(HiveMetrics.TagDropTable.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                client.drop_table(databaseName, tableName, false);
            }
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void rename(final String databaseName,
                       final String oldName,
                       final String newdatabadeName,
                       final String newName) throws TException {
        callWrap(HiveMetrics.TagRename.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                final Table table = client.get_table(databaseName, oldName);
                table.setDbName(newdatabadeName);
                table.setTableName(newName);
                client.alter_table(databaseName, oldName, table);
                EnvironmentContext envContext = new EnvironmentContext(
                    ImmutableMap.of(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE)
                );
                client.alter_table_with_environment_context(databaseName, oldName, table,envContext);
            }
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterTable(final String databaseName,
                           final String tableName,
                           final Table table) throws TException {
        callWrap(HiveMetrics.TagAlterTable.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                client.alter_table(databaseName, tableName, table);
            }
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterDatabase(final String databaseName,
                              final Database database) throws TException {
        callWrap(HiveMetrics.TagAlterDatabase.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                client.alter_database(databaseName, database);
            }
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createDatabase(final Database database) throws TException {
        callWrap(HiveMetrics.TagCreateDatabase.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                client.create_database(database);
            }
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropDatabase(final String dbName) throws TException {
        callWrap(HiveMetrics.TagDropDatabase.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                client.drop_database(dbName, false, false);
            }
            return null;
        });
    }


    /**
     * {@inheritDoc}.
     */
    @Override
    public Database getDatabase(final String databaseName) throws TException {
        return callWrap(HiveMetrics.TagGetDatabase.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                return client.get_database(databaseName);
            }
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<Partition> getPartitions(final String databaseName,
                                         final String tableName,
                                         @Nullable final List<String> partitionNames) throws TException {
        return callWrap(HiveMetrics.TagGetPartitions.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                if (partitionNames != null && !partitionNames.isEmpty()) {
                    return client.get_partitions_by_names(databaseName, tableName, partitionNames);
                } else {
                    return client.get_partitions(databaseName, tableName, ALL_RESULTS);
                }
            }
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropPartitions(final String databaseName,
                               final String tableName,
                               final List<String> partitionNames) throws
        TException {
        callWrap(HiveMetrics.TagDropHivePartitions.getMetricName(), () -> {
            dropHivePartitions(createMetastoreClient(), databaseName, tableName, partitionNames);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<Partition> listPartitionsByFilter(final String databaseName,
                                                  final String tableName,
                                                  final String filter
    ) throws TException {
        return callWrap(HiveMetrics.TagListPartitionsByFilter.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                return client.get_partitions_by_filter(databaseName, tableName, filter, ALL_RESULTS);
            }
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int getPartitionCount(final String databaseName,
                                 final String tableName) throws TException {
        return callWrap(HiveMetrics.TagGetPartitionCount.getMetricName(), () -> {
            return getPartitions(databaseName, tableName, null).size();
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getPartitionNames(final String databaseName,
                                          final String tableName)
        throws TException {
        return callWrap(HiveMetrics.TagGetPartitionNames.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                return client.get_partition_names(databaseName, tableName, ALL_RESULTS);
            }
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void savePartitions(final List<Partition> partitions)
        throws TException {
        callWrap(HiveMetrics.TagAddPartitions.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                client.add_partitions(partitions);
            }
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterPartitions(final String dbName, final String tableName,
                                final List<Partition> partitions) throws
        TException {
        callWrap(HiveMetrics.TagAlterPartitions.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                client.alter_partitions(dbName, tableName, partitions);
            }
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void addDropPartitions(final String dbName, final String tableName,
                                  final List<Partition> partitions,
                                  final List<String> delPartitionNames) throws TException {
        callWrap(HiveMetrics.TagAddDropPartitions.getMetricName(), () -> {
            try (HiveMetastoreClient client = createMetastoreClient()) {
                try {
                    dropHivePartitions(client, dbName, tableName, delPartitionNames);
                    client.add_partitions(partitions);
                } catch (MetaException | InvalidObjectException e) {
                    throw new InvalidMetaException("One or more partitions are invalid.", e);
                } catch (TException e) {
                    throw new TException(
                        String.format("Internal server error adding/dropping partitions for table %s.%s",
                            dbName, tableName), e);
                }
            }
            return null;
        });
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

    private <R> R callWrap(final String requestName, final Callable<R> supplier) throws TException {
        final long start = registry.clock().wallTime();
        final Map<String, String> tags = new HashMap<String, String>();
        tags.put("request", requestName);

        try {
            return supplier.call();
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new TException(e.getMessage(), e.getCause());
        } finally {
            final long duration = registry.clock().wallTime() - start;
            this.registry.timer(requestTimerId.withTags(tags)).record(duration, TimeUnit.MILLISECONDS);
        }
    }

    private void handleSqlException(final Exception ex) {
        if (ex.getCause() instanceof SQLException) {
            this.hiveSqlErrorCounter.increment();
        }
    }

    /**
     * getRoles.
     * @param user user
     * @return set of roles
     */
    public Set<String> getRoles(final String user) {
        return Sets.newHashSet();
    }
}
