package com.netflix.metacat.connector.polaris.store;

import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class exposes APIs for CRUD operations.
 */
@Transactional(rollbackFor = Exception.class)
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class PolarisStoreConnector implements PolarisStoreService {
    private final PolarisDatabaseRepository dbRepo;
    private final PolarisTableRepository tblRepo;

    /**
     * Creates entry for new database.
     * @param databaseName database name
     * @return entity
     */
    @Override
    public PolarisDatabaseEntity createDatabase(final String databaseName) {
        final PolarisDatabaseEntity e = new PolarisDatabaseEntity(databaseName);
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

    /**
     * Fetches all database entities.
     *
     * @return Polaris Database entities
     */
    @Override
    @Transactional(propagation = Propagation.SUPPORTS)
    public List<PolarisDatabaseEntity> getAllDatabases() {
        final int pageFetchSize = 1000;
        final List<PolarisDatabaseEntity> retval = new ArrayList<>();
        Pageable page = PageRequest.of(0, pageFetchSize);
        boolean hasNext;
        do {
            final Slice<PolarisDatabaseEntity> dbs = dbRepo.getDatabases(page);
            retval.addAll(dbs.toList());
            hasNext = dbs.hasNext();
            if (hasNext) {
                page = dbs.nextPageable();
            }
        } while (hasNext);
        return retval;
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
     * @return entity corresponding to created table entry
     */
    @Override
    public PolarisTableEntity createTable(final String dbName, final String tableName, final String metadataLocation) {
        final PolarisTableEntity e = PolarisTableEntity.builder()
            .dbName(dbName)
            .tblName(tableName)
            .metadataLocation(metadataLocation)
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
     * @return table entities in the database.
     */
    @Override
    @Transactional(propagation = Propagation.SUPPORTS)
    public List<PolarisTableEntity> getTableEntities(final String databaseName, final String tableNamePrefix) {
        final int pageFetchSize = 1000;
        final List<PolarisTableEntity> retval = new ArrayList<>();
        final String tblPrefix =  tableNamePrefix == null ? "" : tableNamePrefix;
        Pageable page = PageRequest.of(0, pageFetchSize, Sort.by("tblName").ascending());
        Slice<PolarisTableEntity> tbls;
        boolean hasNext;
        do {
            tbls = tblRepo.findAllTablesByDbNameAndTablePrefix(databaseName, tblPrefix, page);
            retval.addAll(tbls.toList());
            hasNext = tbls.hasNext();
            if (hasNext) {
                page = tbls.nextPageable();
            }
        } while (hasNext);
        return retval;
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
     * @return table names in the database.
     */
    @Override
    @Transactional(propagation = Propagation.SUPPORTS)
    public List<String> getTables(final String databaseName, final String tableNamePrefix) {
        final int pageFetchSize = 1000;
        final List<String> retval = new ArrayList<>();
        final String tblPrefix =  tableNamePrefix == null ? "" : tableNamePrefix;
        Pageable page = PageRequest.of(0, pageFetchSize, Sort.by("tblName").ascending());
        Slice<String> tblNames = null;
        boolean hasNext = true;
        do {
            tblNames = tblRepo.findAllByDbNameAndTablePrefix(databaseName, tblPrefix, page);
            retval.addAll(tblNames.toList());
            hasNext = tblNames.hasNext();
            if (hasNext) {
                page = tblNames.nextPageable();
            }
        } while (hasNext);
        return retval;
    }

    /**
     * Do an atomic compare-and-swap to update the table's metadata location.
     *
     * @param databaseName     database name of the table
     * @param tableName        table name
     * @param expectedLocation expected current metadata-location of the table
     * @param newLocation      new metadata location of the table
     * @return true, if update was successful. false, otherwise.
     */
    @Override
    public boolean updateTableMetadataLocation(final String databaseName, final String tableName,
        final String expectedLocation, final String newLocation) {
        final int updatedRowCount =
            tblRepo.updateMetadataLocation(databaseName, tableName, expectedLocation, newLocation);
        return updatedRowCount > 0;
    }
}
