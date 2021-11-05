package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
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
    @Query("DELETE FROM PolarisTableEntity e WHERE e.dbName = :dbName AND e.tblName = :tblName")
    void deleteByName(
        @Param("dbName") final String dbName,
        @Param("tblName") final String tblName);

    /**
     * Fetch table names in database.
     * @param dbName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @param page pageable.
     * @return table names that belong to the database.
     */
    @Query("SELECT e.tblName FROM PolarisTableEntity e WHERE e.dbName = :dbName AND e.tblName LIKE :tableNamePrefix%")
    Slice<String> findAllByDbNameAndTablePrefix(
        @Param("dbName") final String dbName,
        @Param("tableNamePrefix") final String tableNamePrefix,
        Pageable page);

    /**
     * Fetch table entry.
     * @param dbName database name
     * @param tblName table name
     * @return optional table entry
     */
    Optional<PolarisTableEntity> findByDbNameAndTblName(
        @Param("dbName") final String dbName,
        @Param("tblName") final String tblName);
}
