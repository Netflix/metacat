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

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private volatile List<CatalogThriftService> catalogThriftServices = Collections.emptyList();
    private boolean started;

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
        return catalogThriftServices;
    }

    private List<CatalogThriftService> createCatalogThriftServices() {
        final List<MetacatCatalogConfig> catalogs = connectorManager.getCatalogConfigs()
            .stream()
            .filter(MetacatCatalogConfig::isThriftInterfaceRequested)
            .collect(Collectors.toList());
        final Map<Integer, String> catalogByPort = new HashMap<>();
        for (MetacatCatalogConfig catalog : catalogs) {
            final String conflictingCatalog = catalogByPort.putIfAbsent(
                catalog.getThriftPort(), catalog.getCatalogName());
            if (conflictingCatalog != null) {
                throw new IllegalStateException(String.format(
                    "Catalogs %s and %s are both configured to use thrift port %d",
                    conflictingCatalog, catalog.getCatalogName(), catalog.getThriftPort()));
            }
        }
        return catalogs.stream()
            .map(catalog -> thriftServiceFactory.create(catalog.getCatalogName(), catalog.getThriftPort()))
            .collect(Collectors.toList());
    }

    /**
     * Start.
     *
     * @throws Exception error
     */
    public synchronized void start() throws Exception {
        if (started) {
            return;
        }

        final List<CatalogThriftService> services = createCatalogThriftServices();
        final List<CatalogThriftService> startedServices = new ArrayList<>();
        try {
            for (CatalogThriftService service : services) {
                service.start();
                startedServices.add(service);
            }
        } catch (Exception startException) {
            stopServices(startedServices, startException);
            throw startException;
        }
        catalogThriftServices = Collections.unmodifiableList(services);
        started = true;
    }

    /**
     * Stop.
     *
     * @throws Exception error
     */
    public synchronized void stop() throws Exception {
        if (!started) {
            return;
        }

        Exception stopException = null;
        try {
            stopException = stopServices(catalogThriftServices, null);
        } finally {
            catalogThriftServices = Collections.emptyList();
            started = false;
        }
        if (stopException != null) {
            throw stopException;
        }
    }

    private Exception stopServices(final List<CatalogThriftService> services, final Exception initialException) {
        Exception exception = initialException;
        for (int i = services.size() - 1; i >= 0; i--) {
            try {
                services.get(i).stop();
            } catch (Exception stopException) {
                if (exception == null) {
                    exception = stopException;
                } else {
                    exception.addSuppressed(stopException);
                }
            }
        }
        return exception;
    }

}
