package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

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
    @Transactional
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


    /**
     * Checks if table with the database name and table name exists.
     * @param dbName database name of the table to be looked up.
     * @param tblName table name to be looked up.
     * @return true, if table exists. false, otherwise.
     */
    boolean existsByDbNameAndTblName(
        @Param("dbName") final String dbName,
        @Param("tblName") final String tblName);

    /**
     * Fetch table entities in database.
     * @param dbName database name
     * @param tableNamePrefix table name prefix. can be empty.
     * @param page pageable.
     * @return table entities that belong to the database.
     */
    @Query("SELECT e FROM PolarisTableEntity e WHERE e.dbName = :dbName AND e.tblName LIKE :tableNamePrefix%")
    Slice<PolarisTableEntity> findAllTablesByDbNameAndTablePrefix(
        @Param("dbName") final String dbName,
        @Param("tableNamePrefix") final String tableNamePrefix,
        Pageable page);

    /**
     * Do an atomic compare-and-swap on the metadata location of the table.
     * @param dbName database name of the table
     * @param tableName table name
     * @param expectedLocation expected metadata location before the update is done.
     * @param newLocation new metadata location of the table.
     * @return number of rows that are updated.
     */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("UPDATE PolarisTableEntity t SET t.metadataLocation = :newLocation, "
            + "t.previousMetadataLocation = t.metadataLocation, t.version = t.version + 1 "
            + "WHERE t.metadataLocation = :expectedLocation AND t.dbName = :dbName AND t.tblName = :tableName")
    @Transactional
    int updateMetadataLocation(
        @Param("dbName") final String dbName,
        @Param("tableName") final String tableName,
        @Param("expectedLocation") final String expectedLocation,
        @Param("newLocation") final String newLocation);
}
