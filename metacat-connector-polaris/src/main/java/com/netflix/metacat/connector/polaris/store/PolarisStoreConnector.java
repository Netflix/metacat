package com.netflix.metacat.connector.polaris.store;

import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

/**
 * This class exposes APIs for CRUD operations.
 */
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
     * Updates existing database entity, or creates a new one if not present.
     *
     * @param databaseEntity databaseEntity to save.
     * @return the saved database entity.
     */
    @Override
    public PolarisDatabaseEntity saveDatabase(final PolarisDatabaseEntity databaseEntity) {
        return dbRepo.save(databaseEntity);
    }

    boolean databaseExists(final String dbId) {
        return dbRepo.existsById(dbId);
    }

    /**
     * Creates entry for new table.
     * @param dbName database name
     * @param tableName table name
     * @return entity corresponding to created table entry
     */
    @Override
    public PolarisTableEntity createTable(final String dbName, final String tableName) {
        final PolarisTableEntity e = new PolarisTableEntity(dbName, tableName);
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
    @Transactional
    public void deleteTable(final String dbName, final String tableName) {
        tblRepo.deleteByName(dbName, tableName);
    }

    boolean tableExists(final String tblId) {
        return tblRepo.existsById(tblId);
    }

    /**
     * Fetch table names for given database.
     * @param databaseName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @return table names in the database.
     */
    @Override
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
}
