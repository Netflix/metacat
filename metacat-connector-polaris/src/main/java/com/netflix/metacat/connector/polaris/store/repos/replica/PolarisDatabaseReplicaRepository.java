package com.netflix.metacat.connector.polaris.store.repos.replica;

import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.repos.primary.PolarisDatabaseCustomRepository;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * JPA repository implementation for storing PolarisDatabaseEntity.
 */
@Repository
public interface PolarisDatabaseReplicaRepository
    extends JpaRepository<PolarisDatabaseEntity, String>, PolarisDatabaseCustomRepository {
}
