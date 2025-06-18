package com.netflix.metacat.connector.polaris.store.repos.replica;

import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import com.netflix.metacat.connector.polaris.store.repos.primary.PolarisTableCustomRepository;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * JPA repository implementation for storing PolarisTableEntity.
 */
@Repository
public interface PolarisTableReplicaRepository
    extends JpaRepository<PolarisTableEntity, String>, PolarisTableCustomRepository {

}
