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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.netflix.metacat.common.server.connectors.ConnectorDatabaseService;
import com.netflix.metacat.common.server.connectors.ConnectorPartitionService;
import com.netflix.metacat.common.server.connectors.ConnectorTableService;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * HiveConnectorModule.
 *
 * @author zhenl
 */
@Slf4j

public class HiveConnectorModule implements Module {
    private final String thrifturi = "hive.metastore.uris";
    private final String hivemetastoreTimeout = "hive.metastore-timeout";
    private final String catalogName;
    private final HiveConnectorInfoConverter infoConverter;
    private final HiveMetastoreClientFactory hiveMetastoreClientFactory;
    private URI uri;

    /**
     * Constructor.
     *
     * @param catalogName   catalog name.
     * @param configuration configuration properties
     * @param infoConverter Hive info converter
     */
    public HiveConnectorModule(final String catalogName, final Map<String, String> configuration,
                               final HiveConnectorInfoConverter infoConverter) {
        this.catalogName = catalogName;
        this.infoConverter = infoConverter;
        this.hiveMetastoreClientFactory =
                new HiveMetastoreClientFactory(null,
                        (int) HiveConnectorUtil.toTime(configuration.getOrDefault(hivemetastoreTimeout, "20s"),
                                TimeUnit.SECONDS, TimeUnit.MILLISECONDS));
        final String metastoreUri = configuration.get(thrifturi);
        try {
            this.uri = new URI(metastoreUri);
        } catch (Exception e) {
            final String message = String.format("Invalid thrift uri %s", metastoreUri);
            log.info(message);
            throw new IllegalArgumentException(message, e);
        }
    }

    @Override
    public void configure(final Binder binder) {
        binder.bind(String.class).annotatedWith(Names.named("catalogName")).toInstance(catalogName);
        binder.bind(HiveConnectorInfoConverter.class).toInstance(infoConverter);
        binder.bind(HiveMetastoreClientFactory.class).toInstance(hiveMetastoreClientFactory);
        binder.bind(IMetacatHiveClient.class).to(MetacatHiveClient.class);
        binder.bind(URI.class).annotatedWith(Names.named("thrifturi")).toInstance(uri);
        binder.bind(ConnectorDatabaseService.class).to(HiveConnectorDatabaseService.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorTableService.class).to(HiveConnectorTableService.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPartitionService.class).to(HiveConnectorPartitionService.class).in(Scopes.SINGLETON);
    }

}
