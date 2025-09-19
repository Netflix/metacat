package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * JPA repository implementation for storing PolarisTableEntity.
 */
@Repository
public interface PolarisTableRepository extends JpaRepository<PolarisTableEntity, String>,
    JpaSpecificationExecutor, PolarisTableCustomReplicaRepository, CrudRepository<PolarisTableEntity, String> {

    /**
     * Delete table entry by name.
     * @param catalogName catalog name
     * @param dbName database name.
     * @param tblName table name.
     */
    @Modifying
    @Query("DELETE FROM PolarisTableEntity e "
        + "WHERE e.catalogName = :catalogName AND e.dbName = :dbName AND e.tblName = :tblName")
    @Transactional
    void deleteByName(
        @Param("catalogName") String catalogName,
        @Param("dbName") String dbName,
        @Param("tblName") String tblName);

    /**
     * Fetch table entry.
     * @param catalogName catalog name
     * @param dbName database name
     * @param tblName table name
     * @return optional table entry
     */
    Optional<PolarisTableEntity> findByCatalogNameAndDbNameAndTblName(
        @Param("catalogName") final String catalogName,
        @Param("dbName") final String dbName,
        @Param("tblName") final String tblName);


    /**
     * Checks if table with the database name and table name exists.
     * @param catalogName catalog name
     * @param dbName database name of the table to be looked up.
     * @param tblName table name to be looked up.
     * @return true, if table exists. false, otherwise.
     */
    boolean existsByCatalogNameAndDbNameAndTblName(
        @Param("catalogName") final String catalogName,
        @Param("dbName") final String dbName,
        @Param("tblName") final String tblName);

    /**
     * Do an atomic compare-and-swap on the metadata location of the table.
     * @param catalogName catalog name
     * @param dbName database name of the table
     * @param tableName table name
     * @param expectedLocation expected metadata location before the update is done.
     * @param newLocation new metadata location of the table.
     * @param lastModifiedBy user updating the location.
     * @param lastModifiedDate timestamp for when the location was updated.
     * @return number of rows that are updated.
     */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("UPDATE PolarisTableEntity t SET t.metadataLocation = :newLocation, "
            + "t.audit.lastModifiedBy = :lastModifiedBy, t.audit.lastModifiedDate = :lastModifiedDate, "
            + "t.previousMetadataLocation = t.metadataLocation, t.version = t.version + 1 "
            + "WHERE t.metadataLocation = :expectedLocation "
        + "AND t.catalogName = :catalogName AND t.dbName = :dbName AND t.tblName = :tableName")
    @Transactional
    int updateMetadataLocation(
        @Param("catalogName") final String catalogName,
        @Param("dbName") final String dbName,
        @Param("tableName") final String tableName,
        @Param("expectedLocation") final String expectedLocation,
        @Param("newLocation") final String newLocation,
        @Param("lastModifiedBy") final String lastModifiedBy,
        @Param("lastModifiedDate") final Instant lastModifiedDate);

    /**
     * Do an atomic compare-and-swap on the metadata location and parameters of the table.
     * @param catalogName catalog name
     * @param dbName database name of the table
     * @param tableName table name
     * @param expectedLocation expected metadata location before the update is done.
     * @param newLocation new metadata location of the table.
     * @param newParams new parameters of the table
     * @param lastModifiedBy user updating the location.
     * @param lastModifiedDate timestamp for when the location was updated.
     * @return number of rows that are updated.
     */
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("UPDATE PolarisTableEntity t SET t.metadataLocation = :newLocation, t.params = :newParams,"
        + "t.audit.lastModifiedBy = :lastModifiedBy, t.audit.lastModifiedDate = :lastModifiedDate, "
        + "t.previousMetadataLocation = t.metadataLocation, t.version = t.version + 1 "
        + "WHERE t.metadataLocation = :expectedLocation "
        + "AND t.catalogName = :catalogName AND t.dbName = :dbName AND t.tblName = :tableName")
    @Transactional
    int updateMetadataLocationAndParams(
        @Param("catalogName") final String catalogName,
        @Param("dbName") final String dbName,
        @Param("tableName") final String tableName,
        @Param("expectedLocation") final String expectedLocation,
        @Param("newLocation") final String newLocation,
        @Param("newParams") final Map<String, String> newParams,
        @Param("lastModifiedBy") final String lastModifiedBy,
        @Param("lastModifiedDate") final Instant lastModifiedDate);
}
