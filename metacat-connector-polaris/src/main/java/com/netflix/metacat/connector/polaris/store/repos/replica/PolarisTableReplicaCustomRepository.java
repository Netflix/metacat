package com.netflix.metacat.connector.polaris.store.repos.replica;

import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Custom JPA repository implementation for storing PolarisTableEntity.
 */
public interface PolarisTableReplicaCustomRepository {
    /**
     * Fetch table entities for given database using AS OF SYSTEM TIME follower_read_timestamp().
     * @param dbName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @param pageSize target size for each page
     * @param selectAllColumns if true return the PolarisEntity else return name of the entity
     * @param isAuroraEnabled isAuroraEnabled
     * @return table entities in the database.
     */
    List<?> findAllTablesByDbNameAndTablePrefix(
        String dbName,
        String tableNamePrefix,
        int pageSize,
        boolean selectAllColumns,
        boolean isAuroraEnabled);
}
