package com.netflix.metacat.connector.polaris.store.repos.replica;

import com.netflix.metacat.connector.polaris.store.repos.primary.PolarisTableCustomRepository;
import org.springframework.stereotype.Repository;

/**
 * JPA repository implementation for storing PolarisTableEntity.
 */
@Repository
public interface PolarisTableReplicaRepository extends PolarisTableCustomRepository {

}
