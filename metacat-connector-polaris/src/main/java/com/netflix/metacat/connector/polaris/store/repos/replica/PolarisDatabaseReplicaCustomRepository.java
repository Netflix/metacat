package com.netflix.metacat.connector.polaris.store.repos.replica;

import com.netflix.metacat.common.dto.Sort;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Custom JPA repository implementation for storing PolarisDatabaseEntity.
 */
public interface PolarisDatabaseReplicaCustomRepository {
    /**
     * Fetch db entities for given database using AS OF SYSTEM TIME follower_read_timestamp().
     * @param dbNamePrefix db name prefix. can be empty.
     * @param sort sort
     * @param pageSize db pageSize
     * @param selectAllColumns if true return the PolarisEntity else return name of the entity
     * @param isAuroraEnabled isAuroraEnabled
     * @return table entities in the database.
     */
    List<?> getAllDatabases(
        String dbNamePrefix,
        Sort sort,
        int pageSize,
        boolean selectAllColumns,
        boolean isAuroraEnabled);
}
