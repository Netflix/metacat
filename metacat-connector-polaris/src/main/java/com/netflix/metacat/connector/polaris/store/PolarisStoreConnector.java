package com.netflix.metacat.connector.polaris.store;

import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.connector.polaris.store.entities.AuditEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.repos.primary.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.primary.PolarisTableRepository;
import com.netflix.metacat.connector.polaris.store.repos.replica.PolarisDatabaseReplicaCustomRepository;
import com.netflix.metacat.connector.polaris.store.repos.replica.PolarisDatabaseReplicaRepository;
import com.netflix.metacat.connector.polaris.store.repos.replica.PolarisTableReplicaCustomRepository;
import com.netflix.metacat.connector.polaris.store.repos.replica.PolarisTableReplicaRepository;
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
    private final PolarisDatabaseReplicaRepository replicaDatabaseRepo;
    private final PolarisTableReplicaRepository replicaTableRepo;
    /**
     * Creates entry for new database.
     * @param databaseName database name
     * @return entity
     */
    @Override
    public PolarisDatabaseEntity createDatabase(final String databaseName,
                                                final String location,
                                                final String createdBy) {
        final PolarisDatabaseEntity e = new PolarisDatabaseEntity(databaseName, location, createdBy);
        return dbRepo.save(e);
    }

    /**
     * Fetches database entry.
     *
     * @param databaseName database name
     * @return Polaris Database entity
     */
    @Override
    public Optional<PolarisDatabaseEntity> getDatabase(final String databaseName) {
        return dbRepo.findByDbName(databaseName);
    }

    /**
     * Deletes the database entry.
     *
     * @param dbName database name.
     */
    @Override
    public void deleteDatabase(final String dbName) {
        dbRepo.deleteByName(dbName);
    }

    @Override
    @Transactional(propagation = Propagation.SUPPORTS)
    public List<PolarisDatabaseEntity> getDatabases(
        @Nullable final String dbNamePrefix,
        @Nullable final Sort sort,
        final int pageSize,
        final boolean isAuroraEnabled) {
        return (List<PolarisDatabaseEntity>) replicaDatabaseRepo.getAllDatabases(
            dbNamePrefix,
            sort,
            pageSize,
            true,
            isAuroraEnabled
        );
    }

    @Override
    @Transactional(propagation = Propagation.SUPPORTS)
    public List<String> getDatabaseNames(
        @Nullable final String dbNamePrefix,
        @Nullable final Sort sort,
        final int pageSize,
        final boolean isAuroraEnabled) {
        return (List<String>) replicaDatabaseRepo.getAllDatabases(
            dbNamePrefix,
            sort,
            pageSize,
            false,
            isAuroraEnabled);
    }

    /**
     * Checks if database with the name exists.
     *
     * @param databaseName database name to look up.
     * @return true, if database exists. false, otherwise.
     */
    @Override
    public boolean databaseExists(final String databaseName) {
        return dbRepo.existsByDbName(databaseName);
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
    public PolarisTableEntity createTable(final String dbName,
                                          final String tableName,
                                          final String metadataLocation,
                                          final String createdBy) {
        return createTable(dbName, tableName, metadataLocation, new HashMap<>(), createdBy);
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
    public PolarisTableEntity createTable(final String dbName,
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
    public Optional<PolarisTableEntity> getTable(final String dbName, final String tableName) {
        return tblRepo.findByDbNameAndTblName(dbName, tableName);
    }

    /**
     * Fetch table entities for given database.
     * @param databaseName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @param isAuroraEnabled isAuroraEnabled
     * @return table entities in the database.
     */
    @Override
    @Transactional(propagation = Propagation.SUPPORTS)
    public List<PolarisTableEntity> getTableEntities(final String databaseName,
                                                     final String tableNamePrefix,
                                                     final int pageFetchSize,
                                                     final boolean isAuroraEnabled) {
        return (List<PolarisTableEntity>)
            replicaTableRepo.findAllTablesByDbNameAndTablePrefix(
                databaseName,
                tableNamePrefix,
                pageFetchSize,
                true,
                isAuroraEnabled
            );
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
    public void deleteTable(final String dbName, final String tableName) {
        tblRepo.deleteByName(dbName, tableName);
    }

    /**
     * Checks if table with the name exists.
     *
     * @param tableName table name to look up.
     * @return true, if table exists. false, otherwise.
     */
    @Override
    public boolean tableExists(final String databaseName, final String tableName) {
        return tblRepo.existsByDbNameAndTblName(databaseName, tableName);
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
    @Transactional(propagation = Propagation.SUPPORTS)
    public List<String> getTables(
        final String databaseName,
        final String tableNamePrefix,
        final int pageFetchSize,
        final boolean isAuroraEnabled
    ) {
        return (List<String>)
            replicaTableRepo.findAllTablesByDbNameAndTablePrefix(
                databaseName,
                tableNamePrefix,
                pageFetchSize,
                false,
                isAuroraEnabled
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
            final String databaseName, final String tableName,
            final String expectedLocation, final String newLocation,
            final String lastModifiedBy) {
        final int updatedRowCount =
                tblRepo.updateMetadataLocation(databaseName, tableName,
                        expectedLocation, newLocation, lastModifiedBy, Instant.now());
        return updatedRowCount > 0;
    }

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
    public boolean updateTableMetadataLocationAndParams(
        final String databaseName, final String tableName,
        final String expectedLocation, final String newLocation,
        final Map<String, String> existingParams, final Map<String, String> newParams,
        final String lastModifiedBy) {
        final Map<String, String> mergedParams = existingParams == null
            ? new HashMap<>() : new HashMap<>(existingParams);
        mergedParams.putAll(newParams);
        final int updatedRowCount =
            tblRepo.updateMetadataLocationAndParams(databaseName, tableName,
                expectedLocation, newLocation, mergedParams, lastModifiedBy, Instant.now());
        return updatedRowCount > 0;
    }
}
