package com.netflix.metacat.connector.polaris.configs;

import com.netflix.metacat.connector.polaris.store.PolarisStoreConnector;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.repos.primary.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.primary.PolarisTableRepository;
import com.netflix.metacat.connector.polaris.store.repos.replica.PolarisDatabaseReplicaCustomRepository;
import com.netflix.metacat.connector.polaris.store.repos.replica.PolarisDatabaseReplicaRepository;
import com.netflix.metacat.connector.polaris.store.repos.replica.PolarisTableReplicaCustomRepository;
import com.netflix.metacat.connector.polaris.store.repos.replica.PolarisTableReplicaRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PolarisStoreConfig {
    @Bean
    public PolarisStoreService polarisStoreService(
        final PolarisDatabaseRepository repo,
        final PolarisTableRepository tblRepo,
        final PolarisDatabaseReplicaRepository replicaDatabaseRepo,
        final PolarisTableReplicaRepository replicaTableRepo) {
        return new PolarisStoreConnector(repo, tblRepo, replicaDatabaseRepo, replicaTableRepo);
    }
}
