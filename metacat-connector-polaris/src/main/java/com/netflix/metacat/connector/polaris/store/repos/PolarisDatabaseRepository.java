package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * JPA repository implementation for storing PolarisDatabaseEntity.
 */
@Repository
public interface PolarisDatabaseRepository extends JpaRepository<PolarisDatabaseEntity, String>,
    JpaSpecificationExecutor {

}
