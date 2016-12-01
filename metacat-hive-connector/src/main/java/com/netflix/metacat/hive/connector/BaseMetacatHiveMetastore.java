/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.hive.connector;

import com.facebook.presto.exception.InvalidMetaException;
import com.facebook.presto.exception.SchemaAlreadyExistsException;
import com.facebook.presto.hive.ForHiveMetastore;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCluster;
import com.facebook.presto.hive.HiveErrorCode;
import com.facebook.presto.hive.HiveMetastoreClient;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Hive metastore wrapper.
 */
public class BaseMetacatHiveMetastore extends CachingHiveMetastore implements MetacatHiveMetastore {
    private static final short ALL_RESULTS = -1;

    /**
     * Constructor.
     * @param hiveCluster cluster
     * @param executor executor
     * @param hiveClientConfig config
     */
    @Inject
    public BaseMetacatHiveMetastore(final HiveCluster hiveCluster,
        @ForHiveMetastore
        final ExecutorService executor, final HiveClientConfig hiveClientConfig) {
        super(hiveCluster, executor, hiveClientConfig);
    }

    /**
     * Constructor.
     * @param hiveCluster cluster
     * @param executor executor
     * @param cacheTtl ttl
     * @param refreshInterval refresh interval
     */
    public BaseMetacatHiveMetastore(final HiveCluster hiveCluster, final ExecutorService executor,
        final Duration cacheTtl, final Duration refreshInterval) {
        super(hiveCluster, executor, cacheTtl, refreshInterval);
    }

    @Override
    public void createDatabase(final Database database) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            client.create_database(database);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + database.getName(), e);
        } catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getName(), e);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR,
                "Internal server error creating database", e);
        }
    }

    @Override
    public void updateDatabase(final Database database) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            client.alter_database(database.getName(), database);
        } catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(database.getName(), e);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + database.getName(), e);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Internal server error updating database", e);
        }
    }

    @Override
    public void dropDatabase(final String dbName) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            client.drop_database(dbName, false, false);
        } catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(dbName);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Internal server error dropping database", e);
        }
    }

    @Override
    public void alterTable(final Table table) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            client.alter_table(table.getDbName(), table.getTableName(), table);
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(table.getDbName(), table.getTableName()), e);
        } catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDbName(), table.getTableName()));
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Internal server error altering table", e);
        }
    }

    @Override
    public List<Table> getTablesByNames(final String dbName, final List<String> tableNames) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            return client.get_table_objects_by_name(dbName, tableNames);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Internal server error fetching tables", e);
        }
    }

    @Override
    public List<Partition> getPartitions(final String dbName, final String tableName, final String filter) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            return client.get_partitions_by_filter(dbName, tableName, filter, ALL_RESULTS);
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(dbName, tableName), e);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR,
                String.format("Internal server error fetching partitions for table %s.%s", dbName, tableName), e);
        }
    }

    @Override
    public List<Partition> getPartitions(final String dbName, final String tableName, final List<String> partitionIds) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            if (partitionIds != null && !partitionIds.isEmpty()) {
                return client.get_partitions_by_names(dbName, tableName, partitionIds);
            } else {
                return client.get_partitions(dbName, tableName, ALL_RESULTS);
            }
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(dbName, tableName), e);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR,
                String.format("Internal server error fetching partitions for table %s.%s", dbName, tableName), e);
        }
    }

    @Override
    public void addDropPartitions(final String dbName, final String tableName,
        final List<Partition> partitions,
        final List<String> delPartitionNames) throws NoSuchObjectException, AlreadyExistsException {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            dropHivePartitions(client, dbName, tableName, delPartitionNames);
            client.add_partitions(partitions);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR,
                String.format("Internal server error adding/dropping partitions for table %s.%s", dbName, tableName),
                e);
        }
    }

    @Override
    public void savePartitions(final List<Partition> partitions) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            client.add_partitions(partitions);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Internal server error saving partitions", e);
        }
    }

    @Override
    public void alterPartitions(final String dbName, final String tableName, final List<Partition> partitions) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            client.alter_partitions(dbName, tableName, partitions);
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(dbName, tableName), e);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR,
                String.format("Internal server error altering partitions for table %s.%s", dbName, tableName), e);
        }
    }

    @Override
    public void dropPartitions(final String dbName, final String tableName, final List<String> partitionNames) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
            dropHivePartitions(client, dbName, tableName, partitionNames);
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(dbName, tableName), e);
        } catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR,
                String.format("Internal server error dropping partitions for table %s.%s", dbName, tableName), e);
        }
    }

    private void dropHivePartitions(final HiveMetastoreClient client, final String dbName, final String tableName,
        final List<String> partitionNames)
        throws Exception {
        if (partitionNames != null && !partitionNames.isEmpty()) {
            final DropPartitionsRequest request = new DropPartitionsRequest(dbName, tableName, new RequestPartsSpec(
                RequestPartsSpec._Fields.NAMES, partitionNames));
            request.setDeleteData(false);
            client.drop_partitions_req(request);
        }
    }
}
