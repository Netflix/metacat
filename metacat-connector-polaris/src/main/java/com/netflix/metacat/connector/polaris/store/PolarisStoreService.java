package com.netflix.metacat.connector.polaris.store;

import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Interface methods for Polaris Store CRUD access.
 */
public interface PolarisStoreService {

    /**
     * Creates a database entry.
     * @param databaseName database name
     * @param location the database location.
     * @param createdBy user creating this database.
     * @return Polaris Database entity.
     */
    PolarisDatabaseEntity createDatabase(String databaseName, String location, String createdBy);

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
     * @param dbNamePrefix dbNamePrefix to return
     * @param pageSize db page size
     * @param sort the order of the result
     * @return Polaris Database entities
     */
    List<PolarisDatabaseEntity> getDatabases(String dbNamePrefix, Sort sort, int pageSize);

    /**
     * Fetches all database entities.
     * @param dbNamePrefix dbNamePrefix to return
     * @param sort the order of the result
     * @param pageSize db page size
     * @return Polaris Database entities
     */
    List<String> getDatabaseNames(String dbNamePrefix, Sort sort, int pageSize);

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
     * @param createdBy user creating this table.
     * @return Polaris Table entity.
     */
    PolarisTableEntity createTable(String dbName, String tableName, String metadataLocation, String createdBy);

    /**
     * Creates entry for new table (with parameters).
     * @param dbName database name
     * @param tableName table name
     * @param metadataLocation metadata location of the table.
     * @param params table parameters
     * @param createdBy user creating this table.
     * @return entity corresponding to created table entry
     */
    PolarisTableEntity createTable(String dbName, String tableName,
                                   String metadataLocation, Map<String, String> params, String createdBy);

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
     * @param pageFetchSize  target size for each page
     * @return table entities in the database.
     */
    List<PolarisTableEntity> getTableEntities(String databaseName, String tableNamePrefix, int pageFetchSize);

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
     * @param pageFetchSize size of each page
     * @return list of table names in the database with the table name prefix.
     */
    List<String> getTables(String databaseName, String tableNamePrefix, int pageFetchSize);

    /**
     * Do an atomic compare-and-swap to update the table's metadata location.
     * @param databaseName database name of the table
     * @param tableName table name
     * @param expectedLocation expected current metadata-location of the table
     * @param newLocation new metadata location of the table
     * @param lastModifiedBy   user updating the location.
     * @return true, if update was successful. false, otherwise.
     */
    boolean updateTableMetadataLocation(
        String databaseName, String tableName,
        String expectedLocation, String newLocation,
        String lastModifiedBy);

    /**
     * Do an atomic compare-and-swap to update the table's metdata location and params.
     * @param databaseName database name of the table
     * @param tableName table name
     * @param expectedLocation expected current metadata-location of the table
     * @param newLocation new metadata location of the table
     * @param existingParams current parameters of the table
     * @param newParams new parameters of the table (should only include changed values)
     * @param lastModifiedBy user updating the location
     * @return true, if the location update was successful. false, otherwise
     */
    boolean updateTableMetadataLocationAndParams(
        final String databaseName, final String tableName,
        final String expectedLocation, final String newLocation,
        final Map<String, String> existingParams, final Map<String, String> newParams,
        final String lastModifiedBy
    );
}
