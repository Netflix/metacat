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
     * @param location the database location.
     * @return Polaris Database entity.
     */
    PolarisDatabaseEntity createDatabase(String databaseName, String location);

    /**
     * Fetches database entry.
     * @param databaseName database name
     * @return Polaris Database entity
     */
    Optional<PolarisDatabaseEntity> getDatabase(String databaseName);

    /**
     * Deletes the database entry.
     * @param dbName database name.
     */
    void deleteDatabase(String dbName);

    /**
     * Fetches all database entities.
     * @return Polaris Database entities
     */
    List<PolarisDatabaseEntity> getAllDatabases();

    /**
     * Checks if database with the name exists.
     * @param databaseName database name to look up.
     * @return true, if database exists. false, otherwise.
     */
    boolean databaseExists(String databaseName);

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
     * @param metadataLocation metadata location
     * @return Polaris Table entity.
     */
    PolarisTableEntity createTable(String dbName, String tableName, String metadataLocation);

    /**
     * Fetches table entry.
     * @param dbName database name
     * @param tableName table name
     * @return Polaris Table entity
     */
    Optional<PolarisTableEntity> getTable(String dbName, String tableName);

    /**
     * Fetch table entities for given database.
     * @param databaseName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @return table entities in the database.
     */
    List<PolarisTableEntity> getTableEntities(final String databaseName, final String tableNamePrefix);

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
     * Checks if table with the name exists.
     * @param databaseName database name of the table to be looked up.
     * @param tableName table name to look up.
     * @return true, if table exists. false, otherwise.
     */
    boolean tableExists(String databaseName, String tableName);

    /**
     * Gets tables in the database and tableName prefix.
     * @param databaseName database name
     * @param tableNamePrefix table name prefix
     * @return list of table names in the database with the table name prefix.
     */
    List<String> getTables(String databaseName, String tableNamePrefix);

    /**
     * Do an atomic compare-and-swap to update the table's metadata location.
     * @param databaseName database name of the table
     * @param tableName table name
     * @param expectedLocation expected current metadata-location of the table
     * @param newLocation new metadata location of the table
     * @return true, if update was successful. false, otherwise.
     */
    boolean updateTableMetadataLocation(
        String databaseName, String tableName, String expectedLocation, String newLocation);
}
