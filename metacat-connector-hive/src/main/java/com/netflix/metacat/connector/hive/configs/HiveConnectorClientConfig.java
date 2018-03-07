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
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.client.embedded.EmbeddedHiveClient;
import com.netflix.metacat.connector.hive.metastore.HMSHandlerProxy;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

/**
 * Hive Connector Client Config.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Configuration
@ConditionalOnProperty(value = "useEmbeddedClient", havingValue = "true")
public class HiveConnectorClientConfig {

    /**
     * create local hive client.
     *
     * @param connectorContext connector config context
     * @return IMetacatHiveClient
     * @throws Exception exception
     */
    @Bean
    public IMetacatHiveClient createLocalClient(final ConnectorContext connectorContext) throws Exception {
        try {
            final HiveConf conf = this.getDefaultConf();
            connectorContext.getConfiguration().forEach(conf::set);
            DataSourceManager.get().load(
                connectorContext.getCatalogShardName(),
                connectorContext.getConfiguration()
            );
            return new EmbeddedHiveClient(
                connectorContext.getCatalogName(),
                HMSHandlerProxy.getProxy(conf, connectorContext.getRegistry()),
                connectorContext.getRegistry()
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format(
                    "Failed creating the hive metastore client for catalog: %s",
                    connectorContext.getCatalogName()
                ),
                e
            );
        }
    }


    /**
     * create warehouse for file system calls.
     *
     * @param connectorContext connector config context
     * @return WareHouse
     */
    @Bean
    public Warehouse warehouse(final ConnectorContext connectorContext) {
        try {
            final HiveConf conf = this.getDefaultConf();
            connectorContext.getConfiguration().forEach(conf::set);
            return new Warehouse(conf);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format(
                    "Failed creating the hive warehouse for catalog: %s",
                    connectorContext.getCatalogName()
                ),
                e
            );
        }
    }

    /**
     * hive DataSource.
     *
     * @param connectorContext connector config.
     * @return data source
     */
    @Bean
    public DataSource hiveDataSource(final ConnectorContext connectorContext) {
        final HiveConf conf = this.getDefaultConf();
        connectorContext.getConfiguration().forEach(conf::set);
        DataSourceManager.get().load(
            connectorContext.getCatalogShardName(),
            connectorContext.getConfiguration()
        );
        return DataSourceManager.get().get(connectorContext.getCatalogShardName());
    }

    /**
     * hive metadata Transaction Manager.
     *
     * @param hiveDataSource hive data source
     * @return hive transaction manager
     */
    @Bean
    public DataSourceTransactionManager hiveTxManager(
        @Qualifier("hiveDataSource") final DataSource hiveDataSource) {
        return new DataSourceTransactionManager(hiveDataSource);
    }

    /**
     * hive metadata JDBC template.
     *
     * @param hiveDataSource hive data source
     * @return hive JDBC Template
     */
    @Bean
    public JdbcTemplate hiveJdbcTemplate(
        @Qualifier("hiveDataSource") final DataSource hiveDataSource) {
        return new JdbcTemplate(hiveDataSource);
    }

    private HiveConf getDefaultConf() {
        final HiveConf result = new HiveConf();
        result.setBoolean(HiveConfigConstants.USE_METASTORE_LOCAL, true);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTORETIMEOUT, 60000);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTOREREADTIMEOUT, 60000);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTOREWRITETIMEOUT, 60000);
        result.setInt(HiveConfigConstants.HIVE_METASTORE_DS_RETRY, 0);
        result.setInt(HiveConfigConstants.HIVE_HMSHANDLER_RETRY, 0);
        result.set(
            HiveConfigConstants.JAVAX_JDO_PERSISTENCEMANAGER_FACTORY_CLASS,
            HiveConfigConstants.JAVAX_JDO_PERSISTENCEMANAGER_FACTORY
        );
        result.setBoolean(HiveConfigConstants.HIVE_STATS_AUTOGATHER, false);
        return result;
    }
}
