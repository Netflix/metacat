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
package com.netflix.metacat.main.services;

import com.netflix.metacat.common.server.spi.MetacatCatalogConfig;
import com.netflix.metacat.main.manager.ConnectorManager;
import com.netflix.metacat.thrift.CatalogThriftService;
import com.netflix.metacat.thrift.CatalogThriftServiceFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Metacat thrift service.
 *
 * @author zhenl
 * @since 1.1.0
 */
public class MetacatThriftService {
    private final ConnectorManager connectorManager;
    private final CatalogThriftServiceFactory thriftServiceFactory;

    /**
     * Constructor.
     *
     * @param catalogThriftServiceFactory factory
     * @param connectorManager            connecter manager
     */
    @Inject
    public MetacatThriftService(final CatalogThriftServiceFactory catalogThriftServiceFactory,
                                final ConnectorManager connectorManager) {
        this.thriftServiceFactory = catalogThriftServiceFactory;
        this.connectorManager = connectorManager;
    }

    public List<CatalogThriftService> getCatalogThriftServices() {
        return connectorManager.getCatalogConfigs()
            .stream()
            .filter(MetacatCatalogConfig::isThriftInterfaceRequested)
            .map(catalog -> thriftServiceFactory.create(catalog.getCatalogName(), catalog.getThriftPort()))
            .collect(Collectors.toList());
    }

    /**
     * Start.
     *
     * @throws Exception error
     */
    public void start() throws Exception {
        for (CatalogThriftService service : getCatalogThriftServices()) {
            service.start();
        }
    }

    /**
     * Stop.
     *
     * @throws Exception error
     */
    public void stop() throws Exception {
        for (CatalogThriftService service : getCatalogThriftServices()) {
            service.stop();
        }
    }

}
