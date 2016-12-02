/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.main.init;

import com.google.inject.Inject;
import com.netflix.metacat.main.connector.MetacatConnectorManager;
import com.netflix.metacat.thrift.CatalogThriftService;
import com.netflix.metacat.thrift.CatalogThriftServiceFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Metacat thrift service.
 */
public class MetacatThriftService {
    private final MetacatConnectorManager connectorManager;
    private final CatalogThriftServiceFactory thriftServiceFactory;

    /**
     * Constructor.
     * @param c factory
     * @param m connecter manager
     */
    @Inject
    public MetacatThriftService(final CatalogThriftServiceFactory c, final MetacatConnectorManager m) {
        this.thriftServiceFactory = c;
        this.connectorManager = m;
    }

    protected List<CatalogThriftService> getCatalogThriftServices() {
        return connectorManager.getCatalogs()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().isThriftInterfaceRequested())
            .map(entry -> thriftServiceFactory.create(entry.getKey(), entry.getValue().getThriftPort()))
            .collect(Collectors.toList());
    }

    /**
     * Start.
     * @throws Exception error
     */
    public void start() throws Exception {
        for (CatalogThriftService service : getCatalogThriftServices()) {
            service.start();
        }
    }

    /**
     * Stop.
     * @throws Exception error
     */
    public void stop() throws Exception {
        for (CatalogThriftService service : getCatalogThriftServices()) {
            service.stop();
        }
    }
}
