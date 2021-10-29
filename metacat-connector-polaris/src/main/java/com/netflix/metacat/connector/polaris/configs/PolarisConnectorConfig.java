package com.netflix.metacat.connector.polaris.configs;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.polaris.PolarisConnectorDatabaseService;
import com.netflix.metacat.connector.polaris.store.PolarisStoreConnector;
import com.netflix.metacat.connector.polaris.PolarisConnectorTableService;
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
        final PolarisStoreConnector polarisConnector
    ) {
        return new PolarisConnectorDatabaseService(polarisConnector);
    }

    /**
     * create polaris connector table service.
     *
     * @param polarisConnector          polaris connector
     * @param connectorConverter        connector converter
     * @param connectorDatabaseService  polaris database service
     * @param connectorContext          connector context
     * @return PolarisConnectorTableService
     */
    @Bean
    @ConditionalOnMissingBean(PolarisConnectorTableService.class)
    public PolarisConnectorTableService polarisTableService(
        final PolarisStoreConnector polarisConnector,
        final HiveConnectorInfoConverter connectorConverter,
        final PolarisConnectorDatabaseService connectorDatabaseService,
        final ConnectorContext connectorContext
    ) {
        return new PolarisConnectorTableService(
            polarisConnector,
            connectorContext.getCatalogName(),
            connectorDatabaseService,
            connectorConverter,
            connectorContext
        );
    }
}
