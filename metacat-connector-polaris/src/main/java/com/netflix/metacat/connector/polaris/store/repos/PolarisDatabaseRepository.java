package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
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
    JpaSpecificationExecutor, PolarisDatabaseCustomReplicaRepository {

    /**
     * Fetch database entry.
     * @param catalogName catalog name
     * @param dbName database name
     * @return database entry, if found
     */
    Optional<PolarisDatabaseEntity> findByCatalogNameAndDbName(@Param("catalogName") final String catalogName,
                                                               @Param("dbName") final String dbName);

    /**
     * Check if database with that name exists.
     * @param catalogName catalogName to look up
     * @param dbName database name to look up.
     * @return true, if database exists. false, otherwise.
     */
    boolean existsByCatalogNameAndDbName(
        @Param("catalogName") final String catalogName, @Param("dbName") final String dbName);

    /**
     * Delete database entry by name.
     * @param catalogName catalog name.
     * @param dbName database name.
     */
    @Modifying
    @Query("DELETE FROM PolarisDatabaseEntity e WHERE e.catalogName = :catalogName AND e.dbName = :dbName")
    void deleteByName(@Param("catalogName") String catalogName, @Param("dbName") String dbName);
}
