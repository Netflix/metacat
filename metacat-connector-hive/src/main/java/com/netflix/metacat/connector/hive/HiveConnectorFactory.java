/*
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.netflix.metacat.connector.hive;

import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.common.server.util.MetacatConnectorProperties;
import com.netflix.metacat.connector.hive.client.embedded.EmbeddedHiveClient;
import com.netflix.metacat.connector.hive.client.thrift.HiveMetastoreClientFactory;
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.metastore.HMSHandlerProxy;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import com.netflix.metacat.connector.hive.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * HiveConnectorFactory.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public class HiveConnectorFactory implements ConnectorFactory {
    private final String catalogName;
    private final HiveConnectorInfoConverter infoConverter;
    private final Config config;
    private IMetacatHiveClient client;
    private MetacatConnectorProperties metacatConnectorProperties;
    private HiveConnectorPartitionService partitionService;
    private HiveConnectorTableService tableService;
    private HiveConnectorDatabaseService databaseService;

    /**
     * Constructor.
     *
     * @param config        The system config
     * @param catalogName   connector name. Also the catalog name.
     * @param infoConverter hive info converter
     * @param context       context
     */
    public HiveConnectorFactory(
        final Config config,
        final String catalogName,
        final HiveConnectorInfoConverter infoConverter,
        final MetacatConnectorProperties context
    ) {
        this.config = config;
        this.catalogName = catalogName;
        this.infoConverter = infoConverter;
        this.metacatConnectorProperties = context;
        initHiveServices();
    }

    private void initHiveServices() {
        try {
            final boolean useLocalMetastore = Boolean
                .parseBoolean(metacatConnectorProperties.getConfiguration().getOrDefault(
                    HiveConfigConstants.USE_EMBEDDED_METASTORE, "false"));
            if (!useLocalMetastore) {
                client = createThriftClient();
                if (metacatConnectorProperties.getConfiguration()
                    .containsKey(HiveConfigConstants.USE_FASTHIVE_SERVICE)) {
                    metacatConnectorProperties.getConfiguration()
                        .replace(HiveConfigConstants.USE_FASTHIVE_SERVICE, "false");
                    log.info("HiveConnectorFastService is not used in thrift client mode");
                }
            } else {
                client = createLocalClient();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Failed creating the hive metastore client for catalog: %s", catalogName), e);
        }
        final ApplicationContext ctx = new AnnotationConfigApplicationContext(
            HiveConnectorConfig.class);

        final JdbcUtil jdbcUtil = ctx.getBean(JdbcUtil.class, catalogName, metacatConnectorProperties);

        this.databaseService = ctx.getBean(HiveConnectorDatabaseService.class, catalogName, client, infoConverter);
        final boolean allowRenameTable = Boolean.parseBoolean(
            metacatConnectorProperties.getConfiguration()
                .getOrDefault(HiveConfigConstants.ALLOW_RENAME_TABLE, "false"));

        if (Boolean.parseBoolean(
            metacatConnectorProperties.getConfiguration()
                .getOrDefault(HiveConfigConstants.USE_FASTHIVE_SERVICE, "false"))) {
            this.partitionService = (HiveConnectorFastPartitionService) ctx.getBean("fastHivePartitionService",
                catalogName, client, infoConverter, jdbcUtil, metacatConnectorProperties);
            this.tableService = (HiveConnectorFastTableService) ctx.getBean("fastHiveTableService",
                catalogName, client, infoConverter, this.databaseService,
                jdbcUtil, allowRenameTable, this.metacatConnectorProperties);
        } else {
            this.partitionService = (HiveConnectorPartitionService) ctx.getBean("hivePartitionService",
                catalogName, client, infoConverter);
            this.tableService = (HiveConnectorTableService) ctx.getBean("hiveTableService",
                catalogName, client, infoConverter, this.databaseService, allowRenameTable);
        }
    }

    private IMetacatHiveClient createLocalClient() throws Exception {
        final HiveConf conf = getDefaultConf();
        metacatConnectorProperties.getConfiguration().forEach(conf::set);
        //TODO Change the usage of DataSourceManager later
        DataSourceManager.get().load(catalogName, metacatConnectorProperties.getConfiguration());
        return new EmbeddedHiveClient(catalogName,
            HMSHandlerProxy.getProxy(conf), this.metacatConnectorProperties.getRegistry());

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

    private IMetacatHiveClient createThriftClient() throws MetaException {
        final HiveMetastoreClientFactory factory =
            new HiveMetastoreClientFactory(null,
                (int) HiveConnectorUtil.toTime(
                    metacatConnectorProperties.getConfiguration()
                        .getOrDefault(HiveConfigConstants.HIVE_METASTORE_TIMEOUT, "20s"),
                    TimeUnit.SECONDS, TimeUnit.MILLISECONDS));
        final String metastoreUri = metacatConnectorProperties.getConfiguration().get(HiveConfigConstants.THRIFT_URI);
        URI uri = null;
        try {
            uri = new URI(metastoreUri);
        } catch (Exception e) {
            final String message = String.format("Invalid thrift uri %s", metastoreUri);
            log.info(message);
            throw new IllegalArgumentException(message, e);
        }
        return new MetacatHiveClient(uri, factory);
    }

    @Override
    public ConnectorDatabaseService getDatabaseService() {
        return this.databaseService;
    }

    @Override
    public ConnectorTableService getTableService() {
        return this.tableService;
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        return this.partitionService;
    }

    @Override
    public String getName() {
        return catalogName;
    }

    @Override
    public void stop() {
        try {
            client.shutdown();
        } catch (TException e) {
            log.warn("Failed shutting down the catalog: {}", catalogName, e);
        }
    }
}
