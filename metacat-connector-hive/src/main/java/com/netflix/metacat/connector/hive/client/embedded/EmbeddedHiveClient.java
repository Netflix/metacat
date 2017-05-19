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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.metacat.common.server.monitoring.LogConstants;
import com.netflix.metacat.common.server.partition.util.PartitionUtil;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.metastore.IMetacatHMSHandler;
import com.netflix.spectator.api.Registry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.Database;
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

    private final IMetacatHMSHandler handler;
    private final String catalogName;
    private final Registry registry;

    /**
     * Embedded hive client implementation.
     *
     * @param catalogName catalogName
     * @param handler     handler
     * @param registry    registry
     */
    @Inject
    public EmbeddedHiveClient(@NonNull @Nonnull final String catalogName,
                              final IMetacatHMSHandler handler,
                              @NonNull @Nonnull final Registry registry) {
        this.catalogName = catalogName;
        this.handler = handler;
        this.registry = registry;
    }

    @Override
    public void shutdown() throws TException {
        handler.shutdown();
    }

    private void handleSqlException(final TException ex) {
        if (ex.getCause() instanceof SQLException || ex.getMessage().startsWith(EXCEPTION_JDO_PREFIX)
                || ex.getMessage().contains(EXCEPTION_SQL_PREFIX)
                || ex.getMessage().contains(EX_MESSAGE_RESTART_TRANSACTION)) {
            registry.counter(LogConstants.CounterHiveSqlLockError.name() + "." + catalogName).increment();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createDatabase(@Nonnull @NonNull final Database database) throws TException {
        callWrap(() -> {
            handler.create_database(database);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createTable(@Nonnull @NonNull final Table table) throws TException {
        callWrap(() -> {
            handler.create_table(table);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropDatabase(@Nonnull @NonNull final String dbName) throws TException {
        callWrap(() -> {
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
        callWrap(() -> {
            final List<List<String>> dropParts = new ArrayList<>();
            for (String partName : partitionNames) {
                dropParts.add(new ArrayList<>(PartitionUtil.getPartitionKeyValues(partName).values()));
            }
            handler.add_drop_partitions(dbName, tableName, Lists.newArrayList(), dropParts, false);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterDatabase(@Nonnull @NonNull final String databaseName,
                              @Nonnull @NonNull final Database database) throws TException {
        callWrap(() -> {
            handler.alter_database(databaseName, database);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllDatabases() throws TException {
        return callWrap(handler::get_all_databases);
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
        return callWrap(() -> handler.get_database(databaseName));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllTables(@Nonnull @NonNull final String databaseName) throws TException {
        return callWrap(() -> {
            final List<String> tables = handler.get_all_tables(databaseName);
            if (tables.isEmpty()) {
                handler.get_database(databaseName);
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
        return callWrap(() -> loadTable(databaseName, tableName));
    }

    private Table loadTable(final String dbName, final String tableName) throws TException {
        return callWrap(() -> handler.get_table(dbName, tableName));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterTable(@Nonnull @NonNull final String databaseName,
                           @Nonnull @NonNull final String tableName,
                           @Nonnull @NonNull final Table table) throws TException {
        callWrap(() -> {
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
        callWrap(() -> {
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
        callWrap(() -> {
            final List<List<String>> dropParts = new ArrayList<>();
            for (String partName : dropPartNames) {
                dropParts.add(new ArrayList<>(PartitionUtil.getPartitionKeyValues(partName).values()));
            }
            handler.add_drop_partitions(dbName, tableName, addParts, dropParts, false);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropTable(@Nonnull @NonNull final String databaseName,
                          @Nonnull @NonNull final String tableName) throws TException {
        callWrap(() -> {
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
        callWrap(() -> {
            final Table table = new Table(loadTable(databaseName, oldTableName));
            table.setDbName(newdatabadeName);
            table.setTableName(newTableName);
            handler.alter_table(databaseName, oldTableName, table);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Partition> getPartitions(@Nonnull @NonNull final String databaseName,
                                         @Nonnull @NonNull final String tableName,
                                         @Nullable final List<String> partitionNames) throws TException {
        return callWrap(() -> {
            if (partitionNames != null && !partitionNames.isEmpty()) {
                return handler.get_partitions_by_names(databaseName, tableName, partitionNames);
            }
            return handler.get_partitions(databaseName, tableName, ALL_RESULTS);
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public int getPartitionCount(@Nonnull @NonNull final String databaseName,
                                 @Nonnull @NonNull final String tableName) throws TException {

        return callWrap(() -> getPartitions(databaseName, tableName, null).size());
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<String> getPartitionNames(@Nonnull @NonNull final String databaseName,
                                          @Nonnull @NonNull final String tableName)
            throws TException {
        return callWrap(() -> handler.get_partition_names(databaseName, tableName, ALL_RESULTS));
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
        return callWrap(() -> handler.get_partitions_by_filter(databaseName, tableName, filter, ALL_RESULTS));
    }

    private <R> R callWrap(final Callable<R> supplier) throws TException {
        try {
            return supplier.call();
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new TException(e.getMessage(), e.getCause());
        }
    }
}
