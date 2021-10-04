package com.netflix.metacat.connector.polaris;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/**
 * This class exposes APIs for CRUD operations.
 */
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class PolarisConnector {
    private final PolarisDatabaseRepository repo;

    /**
     * foo bar.
     * @param databaseName db name
     * @return entity
     */
    @Transactional
    public PolarisDatabaseEntity createDatabase(final String databaseName) {
        final PolarisDatabaseEntity e = new PolarisDatabaseEntity(databaseName);
        return repo.save(e);
    }
}
