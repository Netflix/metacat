package com.netflix.metacat.connector.polaris.configs;

import com.netflix.metacat.connector.polaris.store.PolarisStoreConnector;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseRepository;
import com.netflix.metacat.connector.polaris.store.repos.PolarisTableRepository;
import com.netflix.metacat.connector.polaris.store.jdbc.PolarisDatabaseReplicaJDBC;
import com.netflix.metacat.connector.polaris.store.jdbc.PolarisTableReplicaJDBC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;

/**
 * Configuration class for setting up Polaris store-related beans.
 */
@Configuration
public class PolarisStoreConfig {
    /**
     * Creates a PolarisStoreService bean.
     *
     * @param repo the PolarisDatabaseRepository instance
     * @param tblRepo the PolarisTableRepository instance
     * @param replicaDatabaseRepo the PolarisDatabaseReplicaReplicaJDBC instance
     * @param replicaTableRepo the PolarisTableReplicaJDBC instance
     * @return a new instance of PolarisStoreService
     */
    @Bean
    public PolarisStoreService polarisStoreService(
        final PolarisDatabaseRepository repo,
        final PolarisTableRepository tblRepo,
        @Nullable final PolarisDatabaseReplicaJDBC replicaDatabaseRepo,
        @Nullable final PolarisTableReplicaJDBC replicaTableRepo
    ) {
        return new PolarisStoreConnector(
            repo,
            tblRepo,
            replicaDatabaseRepo,
            replicaTableRepo
        );
    }
}
