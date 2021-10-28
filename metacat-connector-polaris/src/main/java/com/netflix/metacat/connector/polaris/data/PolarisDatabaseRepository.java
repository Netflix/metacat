package com.netflix.metacat.connector.polaris.data;

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
