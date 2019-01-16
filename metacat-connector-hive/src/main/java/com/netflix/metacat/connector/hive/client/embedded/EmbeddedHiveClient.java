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
import com.netflix.metacat.common.server.partition.util.PartitionUtil;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.metastore.IMetacatHMSHandler;
import com.netflix.metacat.connector.hive.monitoring.HiveMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
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
     * DEFAULT_PRIVILEGES.
     */
    private static final Set<HivePrivilege> DEFAULT_PRIVILEGES =
        Sets.newHashSet(HivePrivilege.DELETE, HivePrivilege.INSERT, HivePrivilege.SELECT, HivePrivilege.UPDATE);


    /**
     * All results.
     */
    private static final short ALL_RESULTS = -1;
    private final IMetacatHMSHandler handler;
    private final MeterRegistry registry;
    private final Counter hiveSqlErrorCounter;

    /**
     * Embedded hive client implementation.
     *
     * @param catalogName catalogName
     * @param handler     handler
     * @param registry    registry
     */
    public EmbeddedHiveClient(final String catalogName,
                              @Nullable final IMetacatHMSHandler handler,
                              final MeterRegistry registry) {
        this.handler = handler;
        this.registry = registry;
        this.hiveSqlErrorCounter =
            registry.counter(HiveMetrics.CounterHiveSqlLockError.getMetricName() + "." + catalogName);
    }

    @Override
    public void shutdown() throws TException {
        handler.shutdown();
    }

    private void handleSqlException(final TException ex) {
        if ((ex.getCause() instanceof SQLException || ex.getMessage().startsWith(EXCEPTION_JDO_PREFIX)
            || ex.getMessage().contains(EXCEPTION_SQL_PREFIX))
            && ex.getMessage().contains(EX_MESSAGE_RESTART_TRANSACTION)) {
            this.hiveSqlErrorCounter.increment();
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createDatabase(final Database database) throws TException {
        callWrap(HiveMetrics.TagCreateDatabase.getMetricName(), () -> {
            handler.create_database(database);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void createTable(final Table table) throws TException {
        callWrap(HiveMetrics.TagCreateTable.getMetricName(), () -> {
            handler.create_table(table);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void dropDatabase(final String dbName) throws TException {
        callWrap(HiveMetrics.TagDropDatabase.getMetricName(), () -> {
            handler.drop_database(dbName, false, false);
            return null;
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
        dropHivePartitions(databaseName, tableName, partitionNames);
    }

    private void dropHivePartitions(final String dbName, final String tableName,
                                    final List<String> partitionNames)
        throws TException {
        callWrap(HiveMetrics.TagDropHivePartitions.getMetricName(), () -> {
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
    public void alterDatabase(final String databaseName,
                              final Database database) throws TException {
        callWrap(HiveMetrics.TagAlterDatabase.getMetricName(), () -> {
            handler.alter_database(databaseName, database);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllDatabases() throws TException {
        return callWrap(HiveMetrics.TagGetAllDatabases.getMetricName(), handler::get_all_databases);
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
    public Database getDatabase(final String databaseName) throws TException {
        return callWrap(HiveMetrics.TagGetDatabase.getMetricName(), () -> handler.get_database(databaseName));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<String> getAllTables(final String databaseName) throws TException {
        return callWrap(HiveMetrics.TagGetAllTables.getMetricName(), () -> {
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
    public Table getTableByName(final String databaseName,
                                final String tableName) throws TException {
        return callWrap(HiveMetrics.TagGetTableByName.getMetricName(), () -> loadTable(databaseName, tableName));
    }

    private Table loadTable(final String dbName, final String tableName) throws TException {
        return callWrap(HiveMetrics.TagLoadTable.getMetricName(), () -> handler.get_table(dbName, tableName));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterTable(final String databaseName,
                           final String tableName,
                           final Table table) throws TException {
        callWrap(HiveMetrics.TagAlterTable.getMetricName(), () -> {
            handler.alter_table(databaseName, tableName, table);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void alterPartitions(final String dbName,
                                final String tableName,
                                final List<Partition> partitions) throws TException {
        callWrap(HiveMetrics.TagAlterPartitions.getMetricName(), () -> {
            handler.alter_partitions(dbName, tableName, partitions);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void addDropPartitions(final String dbName,
                                  final String tableName,
                                  final List<Partition> addParts,
                                  final List<String> dropPartNames) throws TException {
        callWrap(HiveMetrics.TagAddDropPartitions.getMetricName(), () -> {
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
    public void dropTable(final String databaseName,
                          final String tableName) throws TException {
        callWrap(HiveMetrics.TagDropTable.getMetricName(), () -> {
            handler.drop_table(databaseName, tableName, false);
            return null;
        });
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void rename(final String databaseName,
                       final String oldTableName,
                       final String newdatabadeName,
                       final String newTableName) throws TException {
        callWrap(HiveMetrics.TagRename.getMetricName(), () -> {
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
    public List<Partition> getPartitions(final String databaseName,
                                         final String tableName,
                                         @Nullable final List<String> partitionNames) throws TException {
        return callWrap(HiveMetrics.TagGetPartitions.getMetricName(), () -> {
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
    public int getPartitionCount(final String databaseName,
                                 final String tableName) throws TException {

        return callWrap(HiveMetrics.TagGetPartitionCount.getMetricName(),
            () -> getPartitions(databaseName, tableName, null).size());
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<String> getPartitionNames(final String databaseName,
                                          final String tableName)
        throws TException {
        return callWrap(HiveMetrics.TagGetPartitionNames.getMetricName(),
            () -> handler.get_partition_names(databaseName, tableName, ALL_RESULTS));
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Partition> listPartitionsByFilter(final String databaseName,
                                                  final String tableName,
                                                  final String filter
    ) throws TException {
        return callWrap(HiveMetrics.TagListPartitionsByFilter.getMetricName(),
            () -> handler.get_partitions_by_filter(databaseName, tableName, filter, ALL_RESULTS));
    }

    private <R> R callWrap(final String requestName, final Callable<R> supplier) throws TException {
        final long start = System.currentTimeMillis();
        final Set<Tag> tags = Sets.newHashSet();
        tags.add(Tag.of("request", requestName));

        try {
            return supplier.call();
        } catch (TException e) {
            handleSqlException(e);
            throw e;
        } catch (Exception e) {
            throw new TException(e.getMessage(), e.getCause());
        } finally {
            final long duration = System.currentTimeMillis() - start;
            log.debug("### Time taken to complete {} is {} ms", requestName,
                duration);
            this.registry.timer(HiveMetrics.TimerHiveRequest.getMetricName(), tags)
                .record(duration, TimeUnit.MILLISECONDS);
        }
    }
}
