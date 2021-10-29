package com.netflix.metacat.connector.polaris.configs;

import com.netflix.metacat.connector.polaris.PolarisConnectorDatabaseService;
import com.netflix.metacat.connector.polaris.data.PolarisConnector;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * config for polaris connector.
 */
public class PolarisConnectorConfig {
    /**
     * create polaris connector database service.
     *
     * @param polarisConnector polaris connector
     * @return PolarisConnectorDatabaseService
     */
    @Bean
    @ConditionalOnMissingBean(PolarisConnectorDatabaseService.class)
    public PolarisConnectorDatabaseService polarisDatabaseService(
        final PolarisConnector polarisConnector
    ) {
        return new PolarisConnectorDatabaseService(polarisConnector);
    }
}
