package com.netflix.metacat.connector.polaris;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * JPA repository implementation for storing PolarisTableEntity.
 */
@Repository
public interface PolarisTableRepository extends JpaRepository<PolarisTableEntity, String>,
    JpaSpecificationExecutor {

    /**
     * Delete table entry by name.
     * @param dbName database name.
     * @param tblName table name.
     */
    @Modifying
    @Query("DELETE FROM PolarisTableEntity e WHERE e.dbName = :dbName AND e.tblName = :tblName ")
    void deleteByName(
        @Param("dbName") final String dbName,
        @Param("tblName") final String tblName);
}
