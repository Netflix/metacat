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

package com.netflix.metacat.connector.hive.client.embedded;

import com.github.rholder.retry.AttemptTimeLimiters;
import com.github.rholder.retry.RetryerBuilder;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.monitoring.CounterWrapper;
import com.netflix.metacat.common.partition.util.PartitionUtil;
import com.netflix.metacat.common.server.exception.ConnectorException;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.metastore.MetacatHMSHandler;
import com.netflix.metacat.connector.hive.util.MetacatStopStrategy;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Embedded hive metastore client implementation.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public class EmbeddedHiveClient implements IMetacatHiveClient {
    /**
     * EXCEPTION_JDO_PREFIX.
     */
    public static final String EXCEPTION_JDO_PREFIX = "javax.jdo.";
    /**
     * EXCEPTION_SQL_PREFIX.
     */
    public static final String EXCEPTION_SQL_PREFIX = "java.sql.SQLException";
    /**
     * EX_MESSAGE_RESTART_TRANSACTION.
     */
    public static final String EX_MESSAGE_RESTART_TRANSACTION = "restarting transaction";

    /**
     * retry attempts.
     */
    public static final int RETRY_ATTEMPTS = 3;

    /**
     * attempt execution limit in seconds.
     */
    public static final int MAX_ATTEMPT_TIMEOUT = 30;

    /**
     * DEFAULT_PRIVILEGES.
     */
    private static final Set<HivePrivilege> DEFAULT_PRIVILEGES =
            Sets.newHashSet(HivePrivilege.DELETE, HivePrivilege.INSERT, HivePrivilege.SELECT, HivePrivilege.UPDATE);


    /**
     * All results.
     */
    private static final short ALL_RESULTS = -1;

    private final IHMSHandler handler;
    private final MetacatHMSHandler metacatHMSHandler;
    private final String catalogName;

    /**
     * Embedded hive client implementation.
     *
     * @param catalogName       catalogName
     * @param metacatHMSHandler metacatHMSHandler
     * @param handler           hive metastore handler
     */
    @Inject
    public EmbeddedHiveClient(final String catalogName, final MetacatHMSHandler metacatHMSHandler,
                              final IHMSHandler handler) {
        this.catalogName = catalogName;
        this.handler = handler;
        this.metacatHMSHandler = metacatHMSHandler;
    }

    @Override
    public void shutdown() throws TException {
        handler.shutdown();
    }

    private void handleSqlException(final TException ex) {
        if (ex.getCause() instanceof SQLException || ex.getMessage().startsWith(EXCEPTION_JDO_PREFIX)
                || ex.getMessage().contains(EXCEPTION_SQL_PREFIX)
                || ex.getMessage().contains(EX_MESSAGE_RESTART_TRANSACTION)) {
            CounterWrapper.incrementCounter("dse.metacat." + catalogName + ".sql.lock.error");
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createDatabase(@Nonnull @NonNull final Database database) throws TException {
        retryExec("createDatabase", () -> {
            handler.create_database(database);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createTable(@Nonnull @NonNull final Table table) throws TException {
        retryExec("dropDatabase", () -> {
            handler.create_table(table);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropDatabase(@Nonnull @NonNull final String dbName) throws TException {
        retryExec("dropDatabase", () -> {
            handler.drop_database(dbName, false, false);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropPartitions(@Nonnull @NonNull final String databaseName,
                               @Nonnull @NonNull final String tableName,
                               @Nonnull @NonNull final List<String> partitionNames) throws
            TException {
        dropHivePartitions(databaseName, tableName, partitionNames);
    }

    private void dropHivePartitions(final String dbName, final String tableName,
                                    final List<String> partitionNames)
            throws TException {
        if (partitionNames != null && !partitionNames.isEmpty()) {
            final DropPartitionsRequest request = new DropPartitionsRequest(dbName, tableName, new RequestPartsSpec(
                    RequestPartsSpec._Fields.NAMES, partitionNames));
            request.setDeleteData(false);
            retryExec("dropHivePartitions", () -> {
                handler.drop_partitions_req(request);
                return null;
            });
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterDatabase(@Nonnull @NonNull final String databaseName,
                              @Nonnull @NonNull final Database database) throws TException {
        retryExec("alterDatabase", () -> {
            handler.alter_database(databaseName, database);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllDatabases() throws TException {
        return retryExec("getAllDatabases", handler::get_all_databases);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Set<HivePrivilege> getDatabasePrivileges(final String user, final String databaseName) {
        return DEFAULT_PRIVILEGES;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Database getDatabase(@Nonnull @NonNull final String databaseName) throws TException {
        return retryExec("getDatabase", () -> handler.get_database(databaseName));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllTables(@Nonnull @NonNull final String databaseName) throws TException {
        final Callable<List<String>> getAllTables =
                callableWrap(() -> handler.get_all_tables(databaseName));
        //in case if no table returned check to see if the database exists
        final Callable<Void> getDatabase = callableWrap(() -> {
            handler.get_database(databaseName);
            return null;
        });
        return retryExec("getDatabase", () -> {
            final List<String> tables = getAllTables.call();
            if (tables.isEmpty()) {
                getDatabase.call();
            }
            return tables;
        });

    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Table getTableByName(@Nonnull @NonNull final String databaseName,
                                @Nonnull @NonNull final String tableName) throws TException {
        return loadTable(databaseName, tableName);
    }

    private Table loadTable(final String dbName, final String tableName) throws TException {
        return retryExec("loadTable", () -> handler.get_table(dbName, tableName));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterTable(@Nonnull @NonNull final String databaseName,
                           @Nonnull @NonNull final String tableName,
                           @Nonnull @NonNull final Table table) throws TException {
        retryExec("loadTable", () -> {
            handler.alter_table(databaseName, tableName, table);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterPartitions(@Nonnull @NonNull final String dbName,
                                @Nonnull @NonNull final String tableName,
                                @Nonnull @NonNull final List<Partition> partitions) throws TException {
        retryExec("alterPartitions", () -> {
            handler.alter_partitions(dbName, tableName, partitions);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void addDropPartitions(@Nonnull @NonNull final String dbName,
                                  @Nonnull @NonNull final String tableName,
                                  @Nonnull @NonNull final List<Partition> addParts,
                                  @Nonnull @NonNull final List<String> dropPartNames) throws TException {
        final List<List<String>> dropParts = new ArrayList<>();
        for (String partName : dropPartNames) {
            dropParts.add(new ArrayList<String>(PartitionUtil.getPartitionKeyValues(partName).values()));
        }
        retryExec("addDropPartitions", () -> {
            metacatHMSHandler.add_drop_partitions(dbName, tableName, addParts, dropParts, false);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropTable(@Nonnull @NonNull final String databaseName,
                          @Nonnull @NonNull final String tableName) throws TException {
        retryExec("dropTable", () -> {
            handler.drop_table(databaseName, tableName, false);
            return null;
        });

    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void rename(@Nonnull @NonNull final String databaseName,
                       @Nonnull @NonNull final String oldTableName,
                       @Nonnull @NonNull final String newdatabadeName,
                       @Nonnull @NonNull final String newTableName) throws TException {
        retryExec("rename", () -> {
                    final Table table = new Table(loadTable(databaseName, oldTableName));
                    table.setDbName(newdatabadeName);
                    table.setTableName(newTableName);
                    handler.alter_table(databaseName, oldTableName, table);
                    return null;
                },
                InvalidOperationException.class, MetaException.class, NoSuchObjectException.class);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Partition> getPartitions(@Nonnull @NonNull final String databaseName,
                                         @Nonnull @NonNull final String tableName,
                                         @Nullable final List<String> partitionNames) throws TException {
        if (partitionNames != null && !partitionNames.isEmpty()) {
            return retryExec("rename", () -> handler.get_partitions_by_names(databaseName, tableName, partitionNames),
                    InvalidOperationException.class, MetaException.class, NoSuchObjectException.class);
        }
        return retryExec("rename", () -> handler.get_partitions(databaseName, tableName, ALL_RESULTS),
                InvalidOperationException.class, MetaException.class, NoSuchObjectException.class);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int getPartitionCount(@Nonnull @NonNull final String databaseName,
                                 @Nonnull @NonNull final String tableName) throws TException {

        return getPartitions(databaseName, tableName, null).size();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<String> getPartitionNames(@Nonnull @NonNull final String databaseName,
                                          @Nonnull @NonNull final String tableName)
            throws TException {
        return retryExec("getPartitionNames", () -> handler.get_partition_names(databaseName, tableName, ALL_RESULTS),
                NoSuchObjectException.class);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Partition> listPartitionsByFilter(@Nonnull @NonNull final String databaseName,
                                                  @Nonnull @NonNull final String tableName,
                                                  @Nonnull @NonNull final String filter
    ) throws TException {
        return retryExec("listPartitionsByFilter",
                () -> handler.get_partitions_by_filter(databaseName, tableName, filter, ALL_RESULTS),
                NoSuchObjectException.class);
    }

    /**
     * Wrapper class for retry execution with exception handling.
     * @param functionName functionName
     * @param callable callable
     * @param classes exception class
     * @param <V> v
     * @return function call result, Void in case null
     * @throws TException
     * @throws ConnectorException
     */
    private <V> V retryExec(final String functionName,
                            @Nonnull @NonNull final Callable<V> callable,
                            @NonNull final Class<? extends Exception>... classes)
            throws TException, ConnectorException {
        try {
            return RetryerBuilder.<V>newBuilder()
                    .withAttemptTimeLimiter(
                            AttemptTimeLimiters.<V>fixedTimeLimit(MAX_ATTEMPT_TIMEOUT, TimeUnit.SECONDS))
                    .withStopStrategy(new MetacatStopStrategy()
                            .stopAfterAttemptAndExceptions(RETRY_ATTEMPTS)
                            .stopOn(classes))
                    .build()
                    .call(callableWrap(callable));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TException) {
                handleSqlException((TException) e.getCause());
                throw (TException) e.getCause();
            } else {
                throw new ConnectorException(functionName + " failure", e);
            }
        } catch (Exception e) {
            throw new ConnectorException(functionName + " failure", e);
        }
    }

    /**
     * callableWrap.
     * @param callable callable
     * @param <V> v
     * @return callable
     */
    public static <V> Callable<V> callableWrap(final Callable<V> callable) {
        return new Callable<V>() {
            @Override
            public V call() throws Exception {
                return callable.call();
            }
        };
    }
}
