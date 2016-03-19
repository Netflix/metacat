package com.netflix.metacat.main.connector;

import com.facebook.presto.exception.CatalogNotFoundException;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.main.presto.connector.ConnectorManager;
import com.netflix.metacat.main.presto.index.IndexManager;
import com.netflix.metacat.main.presto.metadata.HandleResolver;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.presto.split.PageSinkManager;
import com.netflix.metacat.main.presto.split.PageSourceManager;
import com.netflix.metacat.main.presto.split.SplitManager;
import com.netflix.metacat.main.spi.MetacatCatalogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.netflix.metacat.main.spi.MetacatCatalogConfig.createFromMapAndRemoveProperties;

@Singleton
public class MetacatConnectorManager extends ConnectorManager {
    private static final Logger log = LoggerFactory.getLogger(MetacatConnectorManager.class);
    private final ConcurrentHashMap<String, MetacatCatalogConfig> catalogs = new ConcurrentHashMap<>();

    @Inject
    public MetacatConnectorManager(MetadataManager metadataManager,
            AccessControlManager accessControlManager,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            IndexManager indexManager,
            PageSinkManager pageSinkManager,
            HandleResolver handleResolver,
            Map<String, ConnectorFactory> connectorFactories,
            NodeManager nodeManager) {
        super(metadataManager, accessControlManager, splitManager, pageSourceManager, indexManager, pageSinkManager, handleResolver,
                connectorFactories, nodeManager);
    }

    @Override
    public synchronized void createConnection(
            String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties) {
        properties = Maps.newHashMap(properties);
        MetacatCatalogConfig config = createFromMapAndRemoveProperties(connectorFactory.getName(), properties);

        super.createConnection(catalogName, connectorFactory, properties);

        catalogs.put(catalogName, config);
    }

    @Nonnull
    public MetacatCatalogConfig getCatalogConfig(QualifiedName name) {
        return getCatalogConfig(name.getCatalogName());
    }

    @Nonnull
    public MetacatCatalogConfig getCatalogConfig(String catalogName) {
        if (Strings.isNullOrEmpty(catalogName)) {
            throw new IllegalArgumentException("catalog-name is required");
        }
        if (!catalogs.containsKey(catalogName)) {
            throw new CatalogNotFoundException(catalogName);
        }
        return catalogs.get(catalogName);
    }

    @Nonnull
    public Map<String, MetacatCatalogConfig> getCatalogs() {
        return ImmutableMap.copyOf(catalogs);
    }
}
