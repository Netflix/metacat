package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * JPA repository implementation for storing PolarisDatabaseEntity.
 */
@Repository
public interface PolarisDatabaseRepository extends JpaRepository<PolarisDatabaseEntity, String>,
    JpaSpecificationExecutor {

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
}
