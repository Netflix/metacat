/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.connector.hive.configs;

import com.netflix.metacat.common.server.connectors.ConnectorContext;
import com.netflix.metacat.common.server.util.ThreadServiceManager;
import com.netflix.metacat.connector.hive.HiveConnectorDatabaseService;
import com.netflix.metacat.connector.hive.HiveConnectorPartitionService;
import com.netflix.metacat.connector.hive.HiveConnectorTableService;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.commonview.CommonViewHandler;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableCriteria;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableCriteriaImpl;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpWrapper;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableOpsProxy;
import com.netflix.metacat.connector.hive.sql.DirectSqlDatabase;
import com.netflix.metacat.connector.hive.sql.DirectSqlGetPartition;
import com.netflix.metacat.connector.hive.sql.DirectSqlSavePartition;
import com.netflix.metacat.connector.hive.sql.DirectSqlTable;
import com.netflix.metacat.connector.hive.sql.HiveConnectorFastDatabaseService;
import com.netflix.metacat.connector.hive.sql.HiveConnectorFastPartitionService;
import com.netflix.metacat.connector.hive.sql.HiveConnectorFastTableService;
import com.netflix.metacat.connector.hive.sql.SequenceGeneration;
import com.netflix.metacat.connector.hive.util.HiveConnectorFastServiceMetric;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * HiveConnectorFastServiceConfig.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Configuration
@EnableTransactionManagement(proxyTargetClass = true)
@ConditionalOnProperty(value = "useHiveFastService", havingValue = "true")
public class HiveConnectorFastServiceConfig {

    /**
     * create hive connector fast service metric.
     *
     * @param connectorContext connector config
     * @return HiveConnectorFastServiceMetric
     */
    @Bean
    public HiveConnectorFastServiceMetric hiveConnectorFastServiceMetric(
        final ConnectorContext connectorContext
    ) {
        return new HiveConnectorFastServiceMetric(
            connectorContext.getRegistry()
        );
    }

    /**
     * create hive connector fast partition service.
     *
     * @param metacatHiveClient      hive client
     * @param warehouse              hive warehouse
     * @param hiveMetacatConverter   metacat converter
     * @param connectorContext       connector config
     * @param directSqlGetPartition  service to get partitions
     * @param directSqlSavePartition service to save partitions
     * @param icebergTableHandler    iceberg table handler
     * @return HiveConnectorPartitionService
     */
    @Bean
    public HiveConnectorPartitionService partitionService(
        final IMetacatHiveClient metacatHiveClient,
        final Warehouse warehouse,
        final HiveConnectorInfoConverter hiveMetacatConverter,
        final ConnectorContext connectorContext,
        final DirectSqlGetPartition directSqlGetPartition,
        final DirectSqlSavePartition directSqlSavePartition,
        final IcebergTableHandler icebergTableHandler
        ) {
        return new HiveConnectorFastPartitionService(
            connectorContext,
            metacatHiveClient,
            warehouse,
            hiveMetacatConverter,
            directSqlGetPartition,
            directSqlSavePartition,
            icebergTableHandler
        );
    }

    /**
     * Service to get partitions.
     *
     * @param threadServiceManager thread service manager
     * @param connectorContext     connector config
     * @param hiveJdbcTemplate     hive JDBC template
     * @param serviceMetric        fast service metric
     * @return HiveConnectorPartitionService
     */
    @Bean
    public DirectSqlGetPartition directSqlGetPartition(
        final ThreadServiceManager threadServiceManager,
        final ConnectorContext connectorContext,
        @Qualifier("hiveReadJdbcTemplate") final JdbcTemplate hiveJdbcTemplate,
        final HiveConnectorFastServiceMetric serviceMetric
    ) {
        return new DirectSqlGetPartition(
            connectorContext,
            threadServiceManager,
            hiveJdbcTemplate,
            serviceMetric
        );
    }

    /**
     * Service to save partitions.
     *
     * @param connectorContext   connector config
     * @param hiveJdbcTemplate   hive JDBC template
     * @param sequenceGeneration sequence generator
     * @param serviceMetric      fast service metric
     * @return HiveConnectorPartitionService
     */
    @Bean
    public DirectSqlSavePartition directSqlSavePartition(
        final ConnectorContext connectorContext,
        @Qualifier("hiveWriteJdbcTemplate") final JdbcTemplate hiveJdbcTemplate,
        final SequenceGeneration sequenceGeneration,
        final HiveConnectorFastServiceMetric serviceMetric
    ) {
        return new DirectSqlSavePartition(
            connectorContext,
            hiveJdbcTemplate,
            sequenceGeneration,
            serviceMetric
        );
    }

    /**
     * Service to generate sequence ids.
     *
     * @param hiveJdbcTemplate hive JDBC template
     * @return HiveConnectorPartitionService
     */
    @Bean
    public SequenceGeneration sequenceGeneration(
        @Qualifier("hiveWriteJdbcTemplate") final JdbcTemplate hiveJdbcTemplate
    ) {
        return new SequenceGeneration(hiveJdbcTemplate);
    }

    /**
     * Data access service for table.
     *
     * @param connectorContext       connector config
     * @param hiveJdbcTemplate       hive JDBC template
     * @param serviceMetric          fast service metric
     * @param directSqlSavePartition partition service involving direct sqls
     * @return DirectSqlTable
     */
    @Bean
    public DirectSqlTable directSqlTable(
        final ConnectorContext connectorContext,
        @Qualifier("hiveWriteJdbcTemplate") final JdbcTemplate hiveJdbcTemplate,
        final HiveConnectorFastServiceMetric serviceMetric,
        final DirectSqlSavePartition directSqlSavePartition
    ) {
        return new DirectSqlTable(
            connectorContext,
            hiveJdbcTemplate,
            serviceMetric,
            directSqlSavePartition
        );
    }

    /**
     * Data access service for database.
     *
     * @param connectorContext       connector config
     * @param hiveJdbcTemplate       hive JDBC template
     * @param serviceMetric          fast service metric
     * @return DirectSqlDatabase
     */
    @Bean
    public DirectSqlDatabase directSqlDatabase(
        final ConnectorContext connectorContext,
        @Qualifier("hiveWriteJdbcTemplate") final JdbcTemplate hiveJdbcTemplate,
        final HiveConnectorFastServiceMetric serviceMetric
    ) {
        return new DirectSqlDatabase(
            connectorContext,
            hiveJdbcTemplate,
            serviceMetric
        );
    }

    /**
     * create hive connector fast table service.
     *
     * @param metacatHiveClient            metacat hive client
     * @param hiveMetacatConverters        hive metacat converters
     * @param hiveConnectorDatabaseService hive database service
     * @param connectorContext             server context
     * @param directSqlTable               table jpa service
     * @param icebergTableHandler          iceberg table handler
     * @param commonViewHandler            common view handler
     * @return HiveConnectorFastTableService
     */
    @Bean
    public HiveConnectorTableService hiveTableService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final HiveConnectorDatabaseService hiveConnectorDatabaseService,
        final ConnectorContext connectorContext,
        final DirectSqlTable directSqlTable,
        final IcebergTableHandler icebergTableHandler,
        final CommonViewHandler commonViewHandler
    ) {
        return new HiveConnectorFastTableService(
            connectorContext.getCatalogName(),
            metacatHiveClient,
            hiveConnectorDatabaseService,
            hiveMetacatConverters,
            connectorContext,
            directSqlTable,
            icebergTableHandler,
            commonViewHandler
        );
    }

    /**
     * create hive connector fast database service.
     *
     * @param metacatHiveClient     metacat hive client
     * @param hiveMetacatConverters hive metacat converters
     * @param directSqlDatabase     database sql service
     * @return HiveConnectorDatabaseService
     */
    @Bean
    public HiveConnectorDatabaseService hiveDatabaseService(
        final IMetacatHiveClient metacatHiveClient,
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final DirectSqlDatabase directSqlDatabase
    ) {
        return new HiveConnectorFastDatabaseService(
            metacatHiveClient,
            hiveMetacatConverters,
            directSqlDatabase
        );
    }

    /**
     * Create iceberg table handler.
     * @param connectorContext      server context
     * @param icebergTableCriteria  iceberg table criteria
     * @param icebergTableOpWrapper iceberg table operation
     * @param icebergTableOpsProxy IcebergTableOps proxy
     * @return IcebergTableHandler
     */
    @Bean
    public IcebergTableHandler icebergTableHandler(final ConnectorContext connectorContext,
                                                   final IcebergTableCriteria icebergTableCriteria,
                                                   final IcebergTableOpWrapper icebergTableOpWrapper,
                                                   final IcebergTableOpsProxy icebergTableOpsProxy) {
        return new IcebergTableHandler(connectorContext,
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
     * Create commonViewHandler.
     *
     * @param connectorContext server context
     * @return CommonViewHandler
     */
    @Bean
    public CommonViewHandler commonViewHandler(final ConnectorContext connectorContext) {
        return new CommonViewHandler(connectorContext);
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
