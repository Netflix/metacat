package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.entities.PolarisAliasEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * JPA repository implementation for storing PolarisAliasEntity.
 */
@Repository
public interface PolarisAliasRepository extends JpaRepository<PolarisAliasEntity, String> {

    /**
     * Resolves an alias to the table it points at.
     *
     * @param catalogName catalog the alias is defined in
     * @param dbName      database the alias is defined in
     * @param aliasName   the alias
     * @return the source table, or empty if the name is not an alias
     */
    @Query("SELECT t FROM PolarisAliasEntity a "
        + "JOIN PolarisDatabaseEntity d ON d.dbId = a.dbId "
        + "JOIN PolarisTableEntity t ON t.tblId = a.sourceTblId "
        + "WHERE d.catalogName = :catalogName "
        + "AND d.dbName = :dbName "
        + "AND a.name = :aliasName")
    Optional<PolarisTableEntity> findSourceTable(
        @Param("catalogName") String catalogName,
        @Param("dbName") String dbName,
        @Param("aliasName") String aliasName);
}
