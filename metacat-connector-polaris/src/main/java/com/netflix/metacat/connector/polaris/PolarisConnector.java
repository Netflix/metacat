package com.netflix.metacat.connector.polaris;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/**
 * This class exposes APIs for CRUD operations.
 */
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class PolarisConnector {
    private final PolarisDatabaseRepository dbRepo;
    private final PolarisTableRepository tblRepo;

    /**
     * Creates entry for new database.
     * @param databaseName database name
     * @return entity
     */
    public PolarisDatabaseEntity createDatabase(final String databaseName) {
        final PolarisDatabaseEntity e = new PolarisDatabaseEntity(databaseName);
        return dbRepo.save(e);
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
    public PolarisTableEntity createTable(final String dbName, final String tableName) {
        final PolarisTableEntity e = new PolarisTableEntity(dbName, tableName);
        return tblRepo.save(e);
    }

    /**
     * Deletes entry for table.
     * @param dbName database name
     * @param tableName table name
     */
    @Transactional
    public void deleteTable(final String dbName, final String tableName) {
        tblRepo.deleteByName(dbName, tableName);
    }

    boolean tableExists(final String tblId) {
        return tblRepo.existsById(tblId);
    }
}
