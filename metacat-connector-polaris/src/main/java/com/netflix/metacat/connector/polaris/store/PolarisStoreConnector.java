package com.netflix.metacat.connector.polaris.store;

import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.connector.polaris.store.entities.AuditEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableRepository;
import com.netflix.metacat.connector.polaris.store.jdbc.PolarisDatabaseReplicaJDBC;
import com.netflix.metacat.connector.polaris.store.jdbc.PolarisTableReplicaJDBC;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class exposes APIs for CRUD operations.
 */
@Transactional(rollbackFor = Exception.class)
@RequiredArgsConstructor
public class PolarisStoreConnector implements PolarisStoreService {
    private final PolarisDatabaseRepository dbRepo;
    private final PolarisTableRepository tblRepo;
    private final PolarisDatabaseReplicaJDBC replicaDatabaseRepo;
    private final PolarisTableReplicaJDBC replicaTableRepo;
    /**
     * Creates entry for new database.
     * @param databaseName database name
     * @return entity
     */
    @Override
    public PolarisDatabaseEntity createDatabase(
        final String catalogName,
                                                final String databaseName,
                                                final String location,
                                                final String createdBy,
                                                final Map<String, String> metadata) {
        final PolarisDatabaseEntity e = new PolarisDatabaseEntity(
            catalogName, databaseName, location, createdBy, metadata);
        return dbRepo.save(e);
    }

    /**
     * Fetches database entry.
     *
     * @param databaseName database name
     * @return Polaris Database entity
     */
    @Override
    public Optional<PolarisDatabaseEntity> getDatabase(final String catalogName, final String databaseName) {
        return dbRepo.findByCatalogNameAndDbName(catalogName, databaseName);
    }

    /**
     * Deletes the database entry.
     * @param catalogName catalogName
     * @param dbName database name.
     */
    @Override
    public void deleteDatabase(final String catalogName, final String dbName) {
        dbRepo.deleteByName(catalogName, dbName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<PolarisDatabaseEntity> getDatabases(
        final String catalogName,
        @Nullable final String dbNamePrefix,
        @Nullable final Sort sort,
        final int pageSize,
        final boolean isAuroraEnabled) {
        return (List<PolarisDatabaseEntity>)
            (isAuroraEnabled
                ? replicaDatabaseRepo.getAllDatabases(catalogName, dbNamePrefix, sort, pageSize, true)
                : dbRepo.getAllDatabases(catalogName, dbNamePrefix, sort, pageSize, true));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<String> getDatabaseNames(
        final String catalogName,
        @Nullable final String dbNamePrefix,
        @Nullable final Sort sort,
        final int pageSize,
        final boolean isAuroraEnabled) {
        return (List<String>) (isAuroraEnabled
            ? replicaDatabaseRepo.getAllDatabases(catalogName, dbNamePrefix, sort, pageSize, false)
            : dbRepo.getAllDatabases(catalogName, dbNamePrefix, sort, pageSize, false)
        );
    }

    /**
     * Checks if database with the name exists.
     *
     * @param databaseName database name to look up.
     * @return true, if database exists. false, otherwise.
     */
    @Override
    public boolean databaseExists(final String catalogName, final String databaseName) {
        return dbRepo.existsByCatalogNameAndDbName(catalogName, databaseName);
    }

    /**
     * Updates existing database entity, or creates a new one if not present.
     *
     * @param databaseEntity databaseEntity to save.
     * @return the saved database entity.
     */
    @Override
    public PolarisDatabaseEntity saveDatabase(final PolarisDatabaseEntity databaseEntity) {
        return dbRepo.save(databaseEntity);
    }

    boolean databaseExistsById(final String dbId) {
        return dbRepo.existsById(dbId);
    }

    /**
     * Creates entry for new table.
     * @param dbName database name
     * @param tableName table name
     * @param metadataLocation metadata location of the table.
     * @param createdBy user creating this table.
     * @return entity corresponding to created table entry
     */
    @Override
    public PolarisTableEntity createTable(
        final String catalogName,
            final String dbName,
                                          final String tableName,
                                          final String metadataLocation,
                                          final String createdBy) {
        return createTable(catalogName, dbName, tableName, metadataLocation, new HashMap<>(), createdBy);
    }

    /**
     * Creates entry for new table (with parameters).
     * @param dbName database name
     * @param tableName table name
     * @param metadataLocation metadata location of the table.
     * @param params table parameters
     * @param createdBy user creating this table.
     * @return entity corresponding to created table entry
     */
    @Override
    public PolarisTableEntity createTable(
        final String catalogName,
        final String dbName,
                                          final String tableName,
                                          final String metadataLocation,
                                          final Map<String, String> params,
                                          final String createdBy) {
        final AuditEntity auditEntity = AuditEntity.builder()
                .createdBy(createdBy)
                .lastModifiedBy(createdBy)
                .build();
        final PolarisTableEntity e = PolarisTableEntity.builder()
                .audit(auditEntity)
            .catalogName(catalogName)
                .dbName(dbName)
                .tblName(tableName)
                .metadataLocation(metadataLocation)
                .params(params)
                .build();
        return tblRepo.save(e);
    }

    /**
     * Fetches table entry.
     *
     * @param tableName table name
     * @return Polaris Table entity
     */
    @Override
    public Optional<PolarisTableEntity> getTable(
        final String catalogName, final String dbName, final String tableName) {
        return tblRepo.findByCatalogNameAndDbNameAndTblName(catalogName, dbName, tableName);
    }

    /**
     * Fetch table entities for given database.
     * @param databaseName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @param isAuroraEnabled isAuroraEnabled
     * @return table entities in the database.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<PolarisTableEntity> getTableEntities(
        final String catalogName,
        final String databaseName,
                                                     final String tableNamePrefix,
                                                     final int pageFetchSize,
                                                     final boolean isAuroraEnabled) {
        return (List<PolarisTableEntity>)
            (isAuroraEnabled
                ? replicaTableRepo.findAllTablesByDbNameAndTablePrefix(
                catalogName,
                databaseName,
                tableNamePrefix,
                pageFetchSize,
                true
            )
                : tblRepo.findAllTablesByDbNameAndTablePrefix(
                catalogName,
                    databaseName,
                    tableNamePrefix,
                    pageFetchSize,
                    true
                ));
    }

    /**
     * Updates existing table entry.
     *
     * @param tableEntity tableEntity to save.
     * @return The saved entity.
     */
    @Override
    public PolarisTableEntity saveTable(final PolarisTableEntity tableEntity) {
        return tblRepo.save(tableEntity);
    }

    /**
     * Deletes entry for table.
     * @param dbName database name
     * @param tableName table name
     */
    @Override
    public void deleteTable(final String catalogName, final String dbName, final String tableName) {
        tblRepo.deleteByName(catalogName, dbName, tableName);
    }

    /**
     * Checks if table with the name exists.
     *
     * @param tableName table name to look up.
     * @return true, if table exists. false, otherwise.
     */
    @Override
    public boolean tableExists(final String catalogName, final String databaseName, final String tableName) {
        return tblRepo.existsByCatalogNameAndDbNameAndTblName(catalogName, databaseName, tableName);
    }

    boolean tableExistsById(final String tblId) {
        return tblRepo.existsById(tblId);
    }

    /**
     * Fetch table names for given database.
     * @param databaseName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @param isAuroraEnabled isAuroraEnabled
     * @return table names in the database.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<String> getTables(
        final String catalogName,
        final String databaseName,
        final String tableNamePrefix,
        final int pageFetchSize,
        final boolean isAuroraEnabled
    ) {
        return (List<String>)
            (isAuroraEnabled
                ? replicaTableRepo.findAllTablesByDbNameAndTablePrefix(
                catalogName,
                databaseName,
                tableNamePrefix,
                pageFetchSize,
                false
                )
                : tblRepo.findAllTablesByDbNameAndTablePrefix(
                catalogName,
                    databaseName,
                    tableNamePrefix,
                    pageFetchSize,
                    false
                )
            );
    }

    /**
     * Do an atomic compare-and-swap to update the table's metadata location.
     *
     * @param databaseName     database name of the table
     * @param tableName        table name
     * @param expectedLocation expected current metadata-location of the table
     * @param newLocation      new metadata location of the table
     * @param lastModifiedBy   user updating the location.
     * @return true, if update was successful. false, otherwise.
     */
    @Override
    public boolean updateTableMetadataLocation(
            final String catalogName,
            final String databaseName, final String tableName,
            final String expectedLocation, final String newLocation,
            final String lastModifiedBy) {
        final int updatedRowCount =
                tblRepo.updateMetadataLocation(catalogName, databaseName, tableName,
                        expectedLocation, newLocation, lastModifiedBy, Instant.now());
        return updatedRowCount > 0;
    }

    /**
     * Do an atomic compare-and-swap to update the table's metdata location and params.
     * @param catalogName catalog name of the table
     * @param databaseName database name of the table
     * @param tableName table name
     * @param expectedLocation expected current metadata-location of the table
     * @param newLocation new metadata location of the table
     * @param existingParams current parameters of the table
     * @param newParams new parameters of the table (should only include changed values)
     * @param lastModifiedBy user updating the location
     * @return true, if the location update was successful. false, otherwise
     */
    public boolean updateTableMetadataLocationAndParams(
        final String catalogName,
        final String databaseName, final String tableName,
        final String expectedLocation, final String newLocation,
        final Map<String, String> existingParams, final Map<String, String> newParams,
        final String lastModifiedBy) {
        final Map<String, String> mergedParams = existingParams == null
            ? new HashMap<>() : new HashMap<>(existingParams);
        mergedParams.putAll(newParams);
        final int updatedRowCount =
            tblRepo.updateMetadataLocationAndParams(catalogName, databaseName, tableName,
                expectedLocation, newLocation, mergedParams, lastModifiedBy, Instant.now());
        return updatedRowCount > 0;
    }
}
