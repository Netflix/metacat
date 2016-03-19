package com.netflix.metacat.main.init;

import com.google.inject.Inject;
import com.netflix.metacat.main.connector.MetacatConnectorManager;
import com.netflix.metacat.thrift.CatalogThriftService;
import com.netflix.metacat.thrift.CatalogThriftServiceFactory;

import java.util.List;
import java.util.stream.Collectors;

public class MetacatThriftService {
    private final MetacatConnectorManager connectorManager;
    private final CatalogThriftServiceFactory thriftServiceFactory;

    @Inject
    public MetacatThriftService(CatalogThriftServiceFactory c, MetacatConnectorManager m) {
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

    public void start() throws Exception {
        for (CatalogThriftService service : getCatalogThriftServices()) {
            service.start();
        }
    }

    public void stop() throws Exception {
        for (CatalogThriftService service : getCatalogThriftServices()) {
            service.stop();
        }
    }
}
