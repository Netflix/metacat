package com.netflix.metacat.connector.polaris.configs;

import com.google.common.collect.ImmutableMap;
import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.commonview.CommonViewHandler;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableCriteria;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableCriteriaImpl;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpWrapper;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpsProxy;
import com.netflix.metacat.connector.polaris.PolarisConnectorDatabaseService;
import com.netflix.metacat.connector.polaris.PolarisConnectorPartitionService;
import com.netflix.metacat.connector.polaris.PolarisConnectorTableService;
import com.netflix.metacat.connector.polaris.common.PolarisConnectorConsts;
import com.netflix.metacat.connector.polaris.common.TransactionRetryAspect;
import com.netflix.metacat.connector.polaris.mappers.PolarisTableMapper;
import com.netflix.metacat.connector.polaris.store.PolarisStoreService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.RetryException;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * Config for polaris connector.
 */
public class PolarisConnectorConfig {
    /**
     * Creates a new instance of a polaris connector partition service.
     *
     * @param icebergTableHandler iceberg table handler
     * @param connectorContext connector context
     * @param polarisTableService polaris table service
     * @return PolarisConnectorPartitionService
     */
    @Bean
    public PolarisConnectorPartitionService polarisConnectorPartitionService(
        final IcebergTableHandler icebergTableHandler,
        final ConnectorContext connectorContext,
        final PolarisConnectorTableService polarisTableService) {
        return new PolarisConnectorPartitionService(connectorContext, icebergTableHandler, polarisTableService);
    }

    /**
     * Create polaris connector database service.
     *
     * @param polarisStoreService polaris store service
     * @param connectorContext    connector context
     * @return PolarisConnectorDatabaseService
     */
    @Bean
    @ConditionalOnMissingBean(PolarisConnectorDatabaseService.class)
    public PolarisConnectorDatabaseService polarisDatabaseService(
        final PolarisStoreService polarisStoreService,
        final ConnectorContext connectorContext
    ) {
        return new PolarisConnectorDatabaseService(polarisStoreService, connectorContext);
    }

    /**
     * Create polaris connector table service.
     *
     * @param polarisStoreService       polaris connector
     * @param connectorConverter        connector converter
     * @param connectorDatabaseService  polaris database service
     * @param icebergTableHandler       iceberg table handler
     * @param commonViewHandler         common view handler
     * @param polarisTableMapper        polaris table mapper
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
        final CommonViewHandler commonViewHandler,
        final PolarisTableMapper polarisTableMapper,
        final ConnectorContext connectorContext
    ) {
        return new PolarisConnectorTableService(
            polarisStoreService,
            connectorContext.getCatalogName(),
            connectorDatabaseService,
            connectorConverter,
            icebergTableHandler,
            commonViewHandler,
            polarisTableMapper,
            connectorContext
        );
    }

    /**
     * Create PolarisTableMapper.
     * @param connectorContext server context
     * @return PolarisTableMapper.
     */
    @Bean
    public PolarisTableMapper polarisTableMapper(final ConnectorContext connectorContext) {
        return new PolarisTableMapper(connectorContext.getCatalogName());
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
     * Create common view handler.
     * @param connectorContext server context
     * @return CommonViewHandler
     */
    @Bean
    public CommonViewHandler commonViewHandler(final ConnectorContext connectorContext) {
        return new CommonViewHandler(connectorContext);
    }

    /**
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

    /**
     * Retry template to use for transaction retries.
     *
     * @return The retry template bean.
     */
    @Bean
    public RetryTemplate transactionRetryTemplate() {
        final RetryTemplate result = new RetryTemplate();
        result.setRetryPolicy(new SimpleRetryPolicy(
                PolarisConnectorConsts.MAX_CRDB_TXN_RETRIES,
                new ImmutableMap.Builder<Class<? extends Throwable>, Boolean>()
                        .put(RetryException.class, true)
                        .build()));
        result.setBackOffPolicy(new ExponentialBackOffPolicy());
        return result;
    }

    /**
     * Aspect advice for transaction retries.
     *
     * @param retryTemplate the transaction retry template.
     * @param connectorContext the connector context.
     * @return TransactionRetryAspect
     */
    @Bean
    public TransactionRetryAspect transactionRetryAspect(
            @Qualifier("transactionRetryTemplate") final RetryTemplate retryTemplate,
            final ConnectorContext connectorContext) {
        return new TransactionRetryAspect(retryTemplate, connectorContext);
    }
}
