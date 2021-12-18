package com.netflix.metacat.connector.polaris.configs;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableCriteria;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableCriteriaImpl;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpWrapper;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpsProxy;
import com.netflix.metacat.connector.polaris.PolarisConnectorDatabaseService;
import com.netflix.metacat.connector.polaris.PolarisConnectorTableService;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Config for polaris connector.
 */
public class PolarisConnectorConfig {
    /**
     * Create polaris connector database service.
     *
     * @param polarisStoreService polaris store service.
     * @return PolarisConnectorDatabaseService
     */
    @Bean
    @ConditionalOnMissingBean(PolarisConnectorDatabaseService.class)
    public PolarisConnectorDatabaseService polarisDatabaseService(
        final PolarisStoreService polarisStoreService
    ) {
        return new PolarisConnectorDatabaseService(polarisStoreService);
    }

    /**
     * Create polaris connector table service.
     *
     * @param polarisStoreService       polaris connector
     * @param connectorConverter        connector converter
     * @param connectorDatabaseService  polaris database service
     * @param icebergTableHandler       iceberg table handler
     * @param connectorContext          connector context
     * @return PolarisConnectorTableService
     */
    @Bean
    @ConditionalOnMissingBean(PolarisConnectorTableService.class)
    public PolarisConnectorTableService polarisTableService(
        final PolarisStoreService polarisStoreService,
        final HiveConnectorInfoConverter connectorConverter,
        final PolarisConnectorDatabaseService connectorDatabaseService,
        final IcebergTableHandler icebergTableHandler,
        final ConnectorContext connectorContext
    ) {
        return new PolarisConnectorTableService(
            polarisStoreService,
            connectorContext.getCatalogName(),
            connectorDatabaseService,
            connectorConverter,
            icebergTableHandler,
            connectorContext
        );
    }

    /**
     * Create iceberg table handler.
     * @param connectorContext      server context
     * @param icebergTableCriteria  iceberg table criteria
     * @param icebergTableOpWrapper iceberg table operation
     * @param icebergTableOpsProxy  IcebergTableOps proxy
     * @return IcebergTableHandler
     */
    @Bean
    public IcebergTableHandler icebergTableHandler(final ConnectorContext connectorContext,
                                                   final IcebergTableCriteria icebergTableCriteria,
                                                   final IcebergTableOpWrapper icebergTableOpWrapper,
                                                   final IcebergTableOpsProxy icebergTableOpsProxy) {
        return new IcebergTableHandler(
            connectorContext,
            icebergTableCriteria,
            icebergTableOpWrapper,
            icebergTableOpsProxy);
    }

    /**
     *
     * Create iceberg table criteria.
     * @param connectorContext server context
     * @return IcebergTableCriteria
     */
    @Bean
    public IcebergTableCriteria icebergTableCriteria(final ConnectorContext connectorContext) {
        return new IcebergTableCriteriaImpl(connectorContext);
    }

    /**
     * Create iceberg table operation wrapper.
     * @param connectorContext     server context
     * @param threadServiceManager executor service
     * @return IcebergTableOpWrapper
     */
    @Bean
    public IcebergTableOpWrapper icebergTableOpWrapper(final ConnectorContext connectorContext,
                                                       final ThreadServiceManager threadServiceManager) {
        return new IcebergTableOpWrapper(connectorContext, threadServiceManager);
    }

    /**
     * Create thread service manager.
     * @param connectorContext connector config
     * @return ThreadServiceManager
     */
    @Bean
    public ThreadServiceManager threadServiceManager(final ConnectorContext connectorContext) {
        return new ThreadServiceManager(connectorContext.getRegistry(), connectorContext.getConfig());
    }

    /**
     * Create IcebergTableOps proxy.
     * @return IcebergTableOpsProxy
     */
    @Bean
    public IcebergTableOpsProxy icebergTableOps() {
        return new IcebergTableOpsProxy();
    }
}
