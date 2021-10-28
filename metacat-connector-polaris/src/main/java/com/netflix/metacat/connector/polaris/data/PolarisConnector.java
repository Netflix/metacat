package com.netflix.metacat.connector.polaris.data;

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

    /**
     * Fetch table names for given database.
     * @param databaseName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @return table names in the database.
     */
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
