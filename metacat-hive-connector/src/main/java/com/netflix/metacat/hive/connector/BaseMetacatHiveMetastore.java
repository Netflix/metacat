package com.netflix.metacat.hive.connector;

import com.facebook.presto.exception.InvalidMetaException;
import com.facebook.presto.exception.SchemaAlreadyExistsException;
import com.facebook.presto.hive.ForHiveMetastore;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCluster;
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

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;

/**
 * Created by amajumdar on 1/16/15.
 */
public class BaseMetacatHiveMetastore extends CachingHiveMetastore implements MetacatHiveMetastore{

    @Inject
    public BaseMetacatHiveMetastore(HiveCluster hiveCluster,
            @ForHiveMetastore
            ExecutorService executor, HiveClientConfig hiveClientConfig) {
        super(hiveCluster, executor, hiveClientConfig);
    }

    public BaseMetacatHiveMetastore(HiveCluster hiveCluster, ExecutorService executor, Duration cacheTtl,
            Duration refreshInterval) {
        super(hiveCluster, executor, cacheTtl, refreshInterval);
    }

    public void createDatabase(Database database){
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            client.create_database(database);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + database.getName(), e);
        } catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getName(), e);
        } catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public void updateDatabase(Database database){
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            client.alter_database(database.getName(), database);
        } catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(database.getName(), e);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("Invalid metadata for " + database.getName(), e);
        } catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public void dropDatabase(String dbName) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            client.drop_database(dbName, false, false);
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(dbName);
        }catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public void alterTable(final Table table) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            client.alter_table(table.getDbName(), table.getTableName(), table);
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(table.getDbName(), table.getTableName()), e);
        } catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDbName(), table.getTableName()));
        } catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<Table> getTablesByNames(String dbName, List<String> tableNames) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            return client.get_table_objects_by_name( dbName, tableNames);
        } catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public List<Partition> getPartitions(String dbName, String tableName, String filter) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            return client.get_partitions_by_filter( dbName, tableName, filter, (short)0);
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(dbName, tableName), e);
        }catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public List<Partition> getPartitions(String dbName, String tableName, List<String> partitionIds) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            return client.get_partitions_by_names( dbName, tableName, partitionIds);
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(dbName, tableName), e);
        }catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addDropPartitions(String dbName, String tableName,
            List<Partition> partitions,
            List<String> delPartitionNames) throws NoSuchObjectException, AlreadyExistsException {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            _dropPartitions( client, dbName, tableName, delPartitionNames);
            client.add_partitions(partitions);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public void savePartitions(List<Partition> partitions) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            client.add_partitions(partitions);
        } catch (MetaException | InvalidObjectException e) {
            throw new InvalidMetaException("One or more partitions are invalid.", e);
        } catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    public void dropPartitions( String dbName, String tableName, List<String> partitionNames) {
        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()){
            _dropPartitions( client, dbName, tableName, partitionNames);
        } catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(dbName, tableName), e);
        }catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private void _dropPartitions(HiveMetastoreClient client, String dbName, String tableName, List<String> partitionNames)
            throws Exception {
        if( partitionNames != null && !partitionNames.isEmpty()) {
            DropPartitionsRequest request = new DropPartitionsRequest(dbName, tableName, new RequestPartsSpec(
                    RequestPartsSpec._Fields.NAMES, partitionNames));
            request.setDeleteData(false);
            client.drop_partitions_req(request);
        }
    }

    @Override
    public void flushCache() {
        //no op
    }
}
