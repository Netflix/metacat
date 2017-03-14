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

import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.util.DataSourceManager;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.client.embedded.EmbeddedHiveClient;
import com.netflix.metacat.connector.hive.metastore.MetacatHMSHandler;
import com.netflix.metacat.connector.hive.client.thrift.HiveMetastoreClientFactory;
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * HiveConnectorFactory.
 * @author zhenl
 */
@Slf4j
public class HiveConnectorFactory implements ConnectorFactory {
    private static final String THRIFT_URI = "hive.metastore.uris";
    private static final String HIVE_METASTORE_TIMEOUT = "hive.metastore-timeout";
    private static final String USE_EMBEDDED_METASTORE = "hive.use.embedded.metastore";
    private final String catalogName;
    private final Map<String, String> configuration;
    private final HiveConnectorInfoConverter infoConverter;
    private IMetacatHiveClient client;
    private ConnectorDatabaseService databaseService;
    private ConnectorTableService tableService;
    private ConnectorPartitionService partitionService;


    /**
     * Constructor.
     * @param catalogName connector name. Also the catalog name.
     * @param configuration configuration properties
     * @param infoConverter hive info converter
     */
    public HiveConnectorFactory(final String catalogName, final Map<String, String> configuration,
                              final HiveConnectorInfoConverter infoConverter) {
        Preconditions.checkNotNull(catalogName, "Catalog name is null");
        Preconditions.checkNotNull(configuration, "Catalog connector configuration is null");
        this.catalogName = catalogName;
        this.configuration = configuration;
        this.infoConverter = infoConverter;
        init();
    }

    private void init() {
        try {
            final boolean useLocalMetastore = Boolean
                .parseBoolean(configuration.getOrDefault(USE_EMBEDDED_METASTORE, "false"));
            if (!useLocalMetastore) {
                client = createThriftClient();
            } else {
                client = createLocalClient();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Failed creating the hive metastore client for catalog: %s", catalogName), e);
        }
        final Module hiveModule = new HiveConnectorModule(catalogName, configuration, infoConverter, client);
        final Injector injector = Guice.createInjector(hiveModule);
        this.databaseService = injector.getInstance(ConnectorDatabaseService.class);
        this.tableService = injector.getInstance(ConnectorTableService.class);
        this.partitionService = injector.getInstance(ConnectorPartitionService.class);
    }

    private IMetacatHiveClient createLocalClient() throws MetaException {
        final HiveConf conf = getDefaultConf();
        configuration.forEach(conf::set);
        DataSourceManager.get().load(catalogName, configuration);
        return new EmbeddedHiveClient(catalogName, new MetacatHMSHandler("metacat", conf, true));
    }

    private static HiveConf getDefaultConf() {
        final HiveConf result = new HiveConf();
        result.setBoolean("hive.metastore.local", true);
        result.setInt("javax.jdo.option.DatastoreTimeout", 60000);
        result.setInt("javax.jdo.option.DatastoreReadTimeoutMillis", 60000);
        result.setInt("javax.jdo.option.DatastoreWriteTimeoutMillis", 60000);
        result.setInt("hive.metastore.ds.retry.attempts", 0);
        result.setInt("hive.hmshandler.retry.attempts", 0);
        result.set("javax.jdo.PersistenceManagerFactoryClass",
                "com.netflix.metacat.connector.hive.client.embedded.HivePersistenceManagerFactory");
        result.setBoolean("hive.stats.autogather", false);
        return result;
    }

    private IMetacatHiveClient createThriftClient() throws MetaException {
        final HiveMetastoreClientFactory factory =
            new HiveMetastoreClientFactory(null,
                (int) HiveConnectorUtil.toTime(configuration.getOrDefault(HIVE_METASTORE_TIMEOUT, "20s"),
                    TimeUnit.SECONDS, TimeUnit.MILLISECONDS));
        final String metastoreUri = configuration.get(THRIFT_URI);
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
        return databaseService;
    }

    @Override
    public ConnectorTableService getTableService() {
        return tableService;
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        return partitionService;
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
            log.warn(String.format("Failed shutting down the catalog: %s", catalogName), e);
        }
    }
}
