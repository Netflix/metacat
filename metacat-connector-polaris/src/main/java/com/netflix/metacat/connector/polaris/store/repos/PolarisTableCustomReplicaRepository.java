package com.netflix.metacat.connector.polaris.store.repos;

import java.util.List;

/**
 * Custom JPA repository implementation for storing PolarisTableEntity.
 */
public interface PolarisTableCustomReplicaRepository {
    /**
     * Fetch table entities for given database using AS OF SYSTEM TIME follower_read_timestamp().
     * @param dbName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @param pageSize target size for each page
     * @param selectAllColumns if true return the PolarisEntity else return name of the entity
     * @param catalogName catalog name
     * @return table entities in the database.
     */
    List<?> findAllTablesByDbNameAndTablePrefix(
        String catalogName,
        String dbName,
        String tableNamePrefix,
        int pageSize,
        boolean selectAllColumns);
}
