package com.netflix.metacat.connector.polaris.store;

import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import java.util.List;
import java.util.Optional;

/**
 * Interface methods for Polaris Store CRUD access.
 */
public interface PolarisStoreService {

    /**
     * Creates a database entry.
     * @param databaseName database name
     * @return Polaris Database entity.
     */
    PolarisDatabaseEntity createDatabase(String databaseName);

    /**
     * Fetches database entry.
     * @param databaseName database name
     * @return Polaris Database entity
     */
    Optional<PolarisDatabaseEntity> getDatabase(String databaseName);

    /**
     * Updates existing database entity.
     * @param databaseEntity databaseEntity to save.
     * @return the saved database entity.
     */
    PolarisDatabaseEntity saveDatabase(PolarisDatabaseEntity databaseEntity);

    /**
     * Creates a table entry.
     * @param dbName database name
     * @param tableName table name
     * @return Polaris Table entity.
     */
    PolarisTableEntity createTable(String dbName, String tableName);

    /**
     * Fetches table entry.
     * @param dbName database name
     * @param tableName table name
     * @return Polaris Table entity
     */
    Optional<PolarisTableEntity> getTable(String dbName, String tableName);

    /**
     * Updates existing or creates new table entry.
     * @param tableEntity tableEntity to save.
     * @return The saved entity.
     */
    PolarisTableEntity saveTable(PolarisTableEntity tableEntity);

    /**
     * Deletes the table entry.
     * @param dbName database name.
     * @param tableName table name.
     */
    void deleteTable(String dbName, String tableName);

    /**
     * Gets tables in the database and tableName prefix.
     * @param databaseName database name
     * @param tableNamePrefix table name prefix
     * @return list of table names in the database with the table name prefix.
     */
    List<String> getTables(String databaseName, String tableNamePrefix);
}
