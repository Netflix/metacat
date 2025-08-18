package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.common.dto.Sort;

import java.util.List;

/**
 * Custom JPA repository implementation for storing PolarisDatabaseEntity.
 */
public interface PolarisDatabaseCustomRepository {
    /**
     * Fetch db entities for given database using AS OF SYSTEM TIME follower_read_timestamp().
     * @param dbNamePrefix db name prefix. can be empty.
     * @param sort sort
     * @param pageSize db pageSize
     * @param selectAllColumns if true return the PolarisEntity else return name of the entity
     * @param auroraEnabled if true auroraEnabled is enabled
     * @return table entities in the database.
     */
    List<?> getAllDatabases(String dbNamePrefix, Sort sort, int pageSize, boolean selectAllColumns,
                            boolean auroraEnabled);
}
