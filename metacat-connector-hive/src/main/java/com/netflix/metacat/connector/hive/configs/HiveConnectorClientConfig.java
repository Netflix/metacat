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

import com.netflix.metacat.common.server.util.ConnectorContext;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.connector.hive.IMetacatHiveClient;
import com.netflix.metacat.connector.hive.client.embedded.EmbeddedHiveClient;
import com.netflix.metacat.connector.hive.metastore.HMSHandlerProxy;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * Hive Connector Client Config.
 * @author zhenl
 * @since 1.1.0
 */
@Configuration
@ConditionalOnProperty(value = "useThriftClient", havingValue = "false")
public class HiveConnectorClientConfig {
    /**
     * create local hive client.
     *
     * @param connectorContext connector config context
     * @return IMetacatHiveClient
     * @throws Exception exception
     */

    @Bean
    public IMetacatHiveClient createLocalClient(
        final ConnectorContext connectorContext) throws Exception {
        try {
            final HiveConf conf = getDefaultConf();
            connectorContext.getConfiguration().forEach(conf::set);
            DataSourceManager.get().load(connectorContext.getCatalogName(),
                connectorContext.getConfiguration());
            return new EmbeddedHiveClient(connectorContext.getCatalogName(),
                HMSHandlerProxy.getProxy(conf), connectorContext.getRegistry());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Failed creating the hive metastore client for catalog: %s",
                    connectorContext.getCatalogName()), e);
        }
    }

    /**
     * hive DataSource.
     *
     * @param connectorContext connector config.
     * @return data source
     */
    @Bean
    //@ConditionalOnProperty(value = "useThriftClient", havingValue = "false")
    public DataSource hiveDataSource(final ConnectorContext connectorContext) {
        final HiveConf conf = getDefaultConf();
        connectorContext.getConfiguration().forEach(conf::set);
        DataSourceManager.get().load(connectorContext.getCatalogName(),
            connectorContext.getConfiguration());
        return DataSourceManager.get().get(connectorContext.getCatalogName());
    }

    private static HiveConf getDefaultConf() {
        final HiveConf result = new HiveConf();
        result.setBoolean(HiveConfigConstants.USE_METASTORE_LOCAL, true);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTORETIMEOUT, 60000);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTOREREADTIMEOUT, 60000);
        result.setInt(HiveConfigConstants.JAVAX_JDO_DATASTOREWRITETIMEOUT, 60000);
        result.setInt(HiveConfigConstants.HIVE_METASTORE_DS_RETRY, 0);
        result.setInt(HiveConfigConstants.HIVE_HMSHANDLER_RETRY, 0);
        result.set(HiveConfigConstants.JAVAX_JDO_PERSISTENCEMANAGER_FACTORY_CLASS,
            HiveConfigConstants.JAVAX_JDO_PERSISTENCEMANAGER_FACTORY);
        result.setBoolean(HiveConfigConstants.HIVE_STATS_AUTOGATHER, false);
        return result;
    }
}
