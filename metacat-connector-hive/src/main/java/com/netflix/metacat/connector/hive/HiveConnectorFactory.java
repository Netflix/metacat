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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorFactory;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.common.server.properties.Config;
import com.netflix.metacat.common.server.util.DataSourceManager;
import com.netflix.metacat.connector.hive.client.embedded.EmbeddedHiveClient;
import com.netflix.metacat.connector.hive.client.thrift.HiveMetastoreClientFactory;
import com.netflix.metacat.connector.hive.client.thrift.MetacatHiveClient;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.metastore.HMSHandlerProxy;
import com.netflix.metacat.connector.hive.util.HiveConfigConstants;
import com.netflix.spectator.api.Registry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Map;
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
    private final Map<String, String> configuration;
    private final HiveConnectorInfoConverter infoConverter;
    private final Injector injector;
    private IMetacatHiveClient client;
    private final Registry registry;

    /**
     * Constructor.
     *
     * @param config        The system config
     * @param catalogName   connector name. Also the catalog name.
     * @param configuration configuration properties
     * @param infoConverter hive info converter
     * @param registry      registry to spectator
     */
    public HiveConnectorFactory(
        @Nonnull @NonNull final Config config,
        @Nonnull @NonNull final String catalogName,
        @Nonnull @NonNull final Map<String, String> configuration,
        final HiveConnectorInfoConverter infoConverter,
        @Nonnull @NonNull final Registry registry
    ) {
        this.catalogName = catalogName;
        this.configuration = configuration;
        this.infoConverter = infoConverter;
        this.registry = registry;

        try {
            final boolean useLocalMetastore = Boolean
                .parseBoolean(configuration.getOrDefault(HiveConfigConstants.USE_EMBEDDED_METASTORE, "false"));
            if (!useLocalMetastore) {
                client = createThriftClient();
                if (configuration.containsKey(HiveConfigConstants.USE_FASTHIVE_SERVICE)) {
                    configuration.replace(HiveConfigConstants.USE_FASTHIVE_SERVICE, "false");
                    log.info("Always not use HiveConnectorFastService in thrift client mode");
                }
            } else {
                client = createLocalClient();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Failed creating the hive metastore client for catalog: %s", catalogName), e);
        }
        final Module hiveModule = new HiveConnectorModule(config,
            catalogName, configuration, infoConverter, client, registry);
        this.injector = Guice.createInjector(hiveModule);
    }

    private IMetacatHiveClient createLocalClient() throws Exception {
        final HiveConf conf = getDefaultConf();
        configuration.forEach(conf::set);
        //TO DO Change the usage of DataSourceManager later
        DataSourceManager.get().load(catalogName, configuration);
        return new EmbeddedHiveClient(catalogName, HMSHandlerProxy.getProxy(conf), registry);

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
                    configuration.getOrDefault(HiveConfigConstants.HIVE_METASTORE_TIMEOUT, "20s"),
                    TimeUnit.SECONDS, TimeUnit.MILLISECONDS));
        final String metastoreUri = configuration.get(HiveConfigConstants.THRIFT_URI);
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
        return this.injector.getInstance(ConnectorDatabaseService.class);
    }

    @Override
    public ConnectorTableService getTableService() {
        return this.injector.getInstance(ConnectorTableService.class);
    }

    @Override
    public ConnectorPartitionService getPartitionService() {
        return this.injector.getInstance(ConnectorPartitionService.class);
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
