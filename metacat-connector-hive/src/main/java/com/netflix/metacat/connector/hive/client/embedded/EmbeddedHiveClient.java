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

import com.google.common.collect.Sets;
import com.netflix.metacat.common.monitoring.CounterWrapper;
import com.netflix.metacat.common.partition.util.PartitionUtil;
import com.netflix.metacat.common.server.exception.ConnectorException;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.metastore.MetacatHMSHandler;
import com.netflix.metacat.connector.hive.util.RetryHelper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
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

/**
 * Embedded hive metastore client implementation.
 *
 * @author zhenl
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
     * DEFAULT_PRIVILEGES.
     */
    static final Set<HivePrivilege> DEFAULT_PRIVILEGES =
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
    public void createDatabase(@NonNull final Database database) throws TException {
        try {
            handler.create_database(database);
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createTable(@NonNull final Table table) throws TException {
        try {
            handler.create_table(table);
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropDatabase(@NonNull final String dbName) throws TException {
        try {
            handler.drop_database(dbName, false, false);
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterDatabase(@NonNull final String databaseName,
                              @NonNull final Database database) throws TException {
        try {
            handler.alter_database(databaseName, database);
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllDatabases() throws TException {
        try {
            return RetryHelper.retry()
                    .maxAttempts(RETRY_ATTEMPTS)
                    .stopOnIllegalExceptions()
                    .run("getAllDatabases", RetryHelper.callableWrap(() -> handler.get_all_databases()));
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("getAllDatabases failure", e);
        }
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
    public Database getDatabase(@NonNull final String databaseName) throws TException {
        try {
            return RetryHelper.retry()
                    .maxAttempts(RETRY_ATTEMPTS)
                    .stopOnIllegalExceptions()
                    .run("getDatabase", RetryHelper.callableWrap(() -> handler.get_database(databaseName)));
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("getDatabase failure", e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllTables(@Nonnull final String databaseName) throws TException {
        final Callable<List<String>> getAllTables =
                RetryHelper.callableWrap(() -> handler.get_all_tables(databaseName));
        //in case if no table returned check to see if the database exists
        final Callable<Void> getDatabase = RetryHelper.callableWrap(() -> {
            handler.get_database(databaseName);
            return null;
        });
        try {
            return RetryHelper.retry()
                    .maxAttempts(RETRY_ATTEMPTS)
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllTables", () -> {
                        final List<String> tables = getAllTables.call();
                        if (tables.isEmpty()) {
                            getDatabase.call();
                        }
                        return tables;
                    });
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("getAllTables failure", e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Table getTableByName(@Nonnull final String databaseName,
                                @NonNull final String tableName) throws TException {
        try {
            return loadTable(databaseName, tableName);
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("getTableByName failure", e);
        }
    }

    private Table loadTable(final String dbName, final String tableName) throws Exception {
        return RetryHelper.retry()
                .maxAttempts(RETRY_ATTEMPTS)
                .stopOn(NoSuchObjectException.class)
                .stopOnIllegalExceptions()
                .run("getTable", RetryHelper.callableWrap(() -> {
                    return handler.get_table(dbName, tableName);
                }));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterTable(@NonNull final String databaseName,
                           @NonNull final String tableName,
                           @NonNull final Table table) throws TException {
        try {
            handler.alter_table(databaseName, tableName, table);
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("alterTable failure", e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterPartitions(@Nonnull final String dbName, @Nonnull final String tableName,
                                @Nonnull final List<Partition> partitions) throws TException {
        try {
            handler.alter_partitions(dbName, tableName, partitions);
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("alterPartitions failure", e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void addDropPartitions(@Nonnull final String dbName,
                                  @Nonnull final String tableName,
                                  @Nonnull final List<Partition> addParts,
                                  @Nonnull final List<String> dropPartNames) throws TException {

        try {
            final List<List<String>> dropParts = new ArrayList<>();
            for (String partName : dropPartNames) {
                dropParts.add(new ArrayList<String>(PartitionUtil.getPartitionKeyValues(partName).values()));
            }
            metacatHMSHandler.add_drop_partitions(dbName, tableName, addParts, dropParts, false);
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("alterPartitions failure", e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropTable(@Nonnull final String databaseName, @NonNull final String tableName) throws TException {
        try {
            handler.drop_table(databaseName, tableName, false);
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("dropTable failure", e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void rename(@Nonnull final String databaseName,
                       @NonNull final String oldTableName,
                       @Nonnull final String newdatabadeName,
                       @Nonnull final String newTableName) throws TException {
        try {
            RetryHelper.retry()
                    .maxAttempts(RETRY_ATTEMPTS)
                    .stopOn(InvalidOperationException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("renameTable", RetryHelper.callableWrap(() -> {
                        final Table table = new Table(loadTable(databaseName, oldTableName));
                        table.setDbName(newdatabadeName);
                        table.setTableName(newTableName);
                        handler.alter_table(databaseName, oldTableName, table);
                        return null;
                    }));
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("dropTable failure", e);
        }

    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<Partition> getPartitions(@Nonnull final String databaseName,
                                         @NonNull final String tableName,
                                         @Nullable final List<String> partitionNames) throws TException {
        try {
            if (partitionNames != null && !partitionNames.isEmpty()) {
                return RetryHelper.retry()
                        .maxAttempts(RETRY_ATTEMPTS)
                        .stopOn(NoSuchObjectException.class)
                        .stopOnIllegalExceptions()
                        .run("getPartitionsByNames", RetryHelper.callableWrap(() ->
                                handler.get_partitions_by_names(databaseName, tableName, partitionNames)));
            } else {
                return RetryHelper.retry()
                        .maxAttempts(RETRY_ATTEMPTS)
                        .stopOn(NoSuchObjectException.class)
                        .stopOnIllegalExceptions()
                        .run("getPartitionsByNames", RetryHelper.callableWrap(() ->
                                handler.get_partitions(databaseName, tableName, ALL_RESULTS)));
            }
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("getPartitions failure", e);
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
        try {
            return RetryHelper.retry()
                    .maxAttempts(RETRY_ATTEMPTS)
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", RetryHelper.callableWrap(() ->
                            handler.get_partition_names(databaseName, tableName, ALL_RESULTS)));
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("getPartitionNames failure", e);
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<Partition> listPartitionsByFilter(@Nonnull final String databaseName,
                                                  @NonNull final String tableName,
                                                  @Nonnull final String filter
    ) throws TException {
        try {
            return RetryHelper.retry()
                    .maxAttempts(RETRY_ATTEMPTS)
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("listPartitionsByFilter", RetryHelper.callableWrap(() ->
                            handler.get_partitions_by_filter(databaseName, tableName, filter, ALL_RESULTS)));
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new ConnectorException("getPartitionNames failure", e);
        }
    }
}
