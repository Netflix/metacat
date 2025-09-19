package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * JPA repository implementation for storing PolarisDatabaseEntity.
 */
@Repository
public interface PolarisDatabaseRepository extends JpaRepository<PolarisDatabaseEntity, String>,
    CrudRepository<PolarisDatabaseEntity, String>,
    JpaSpecificationExecutor, PolarisDatabaseCustomRepository {

    /**
     * Fetch database entry.
     * @param dbName database name
     * @return database entry, if found
     */
    Optional<PolarisDatabaseEntity> findByDbName(@Param("dbName") final String dbName);

    /**
     * Check if database with that name exists.
     * @param dbName database name to look up.
     * @return true, if database exists. false, otherwise.
     */
    boolean existsByDbName(@Param("dbName") final String dbName);

    /**
     * Delete database entry by name.
     * @param dbName database name.
     */
    @Modifying
    @Query("DELETE FROM PolarisDatabaseEntity e WHERE e.dbName = :dbName")
    void deleteByName(@Param("dbName") final String dbName);

    /**
     * Fetch databases.
     * @param page pageable page.
     * @return database entities.
     */
    @Query("SELECT e FROM PolarisDatabaseEntity e")
    Slice<PolarisDatabaseEntity> getDatabases(Pageable page);
}
