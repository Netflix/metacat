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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.metacat.main.presto.connector;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.base.Preconditions;
import com.netflix.metacat.main.presto.metadata.HandleResolver;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.presto.split.SplitManager;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connector manager.
 */
@Slf4j
public class ConnectorManager {
    private final MetadataManager metadataManager;
    private final SplitManager splitManager;
    private final HandleResolver handleResolver;

    private final ConcurrentMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Connector> connectors = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();

    /**
     * Constructor.
     * @param metadataManager metadata manager
     * @param splitManager splt manager
     * @param handleResolver resolver
     * @param connectorFactories connector factories
     */
    @Inject
    public ConnectorManager(final MetadataManager metadataManager,
        final SplitManager splitManager,
        final HandleResolver handleResolver,
        final Map<String, ConnectorFactory> connectorFactories) {
        this.metadataManager = metadataManager;
        this.splitManager = splitManager;
        this.handleResolver = handleResolver;
        this.connectorFactories.putAll(connectorFactories);
    }

    /**
     * Stop.
     */
    @PreDestroy
    public void stop() {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (Map.Entry<String, Connector> entry : connectors.entrySet()) {
            final Connector connector = entry.getValue();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
                connector.getClass().getClassLoader())) {
                connector.shutdown();
            } catch (Throwable t) {
                log.error(String.format("Error shutting down connector: %s", entry.getKey()), t);
            }
        }
    }

    /**
     * Adds connector factory.
     * @param connectorFactory factory
     */
    public void addConnectorFactory(final ConnectorFactory connectorFactory) {
        Preconditions.checkState(!stopped.get(), "ConnectorManager is stopped");
        final ConnectorFactory existingConnectorFactory = connectorFactories
            .putIfAbsent(connectorFactory.getName(), connectorFactory);
        Preconditions.checkArgument(existingConnectorFactory == null, "Connector %s is already registered",
            connectorFactory.getName());
    }

    /**
     * Creates a connection for the given catalog.
     * @param catalogName catalog name
     * @param connectorName connector name
     * @param properties properties
     */
    public synchronized void createConnection(final String catalogName, final String connectorName,
        final Map<String, String> properties) {
        Preconditions.checkState(!stopped.get(), "ConnectorManager is stopped");
        Preconditions.checkNotNull(catalogName, "catalogName is null");
        Preconditions.checkNotNull(connectorName, "connectorName is null");
        Preconditions.checkNotNull(properties, "properties is null");

        final ConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        Preconditions.checkArgument(connectorFactory != null, "No factory for connector %s", connectorName);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(
            connectorFactory.getClass().getClassLoader())) {
            createConnection(catalogName, connectorFactory, properties);
        }
    }

    /**
     * Creates a connection.
     * @param catalogName catalog name
     * @param connectorFactory factory
     * @param properties properties
     */
    public synchronized void createConnection(final String catalogName, final ConnectorFactory connectorFactory,
        final Map<String, String> properties) {
        Preconditions.checkState(!stopped.get(), "ConnectorManager is stopped");
        Preconditions.checkNotNull(catalogName, "catalogName is null");
        Preconditions.checkNotNull(properties, "properties is null");
        Preconditions.checkNotNull(connectorFactory, "connectorFactory is null");

        final String connectorId = getConnectorId(catalogName);
        Preconditions.checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);

        final Connector connector = connectorFactory.create(connectorId, properties);

        addConnector(catalogName, connectorId, connector);
    }

    private synchronized void addConnector(final String catalogName, final String connectorId,
        final Connector connector) {
        Preconditions.checkState(!stopped.get(), "ConnectorManager is stopped");
        Preconditions.checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);
        connectors.put(connectorId, connector);

        final ConnectorMetadata connectorMetadata = connector.getMetadata();
        Preconditions.checkState(connectorMetadata != null, "Connector %s can not provide metadata", connectorId);

        final ConnectorSplitManager connectorSplitManager = connector.getSplitManager();
        Preconditions
            .checkState(connectorSplitManager != null, "Connector %s does not have a split manager", connectorId);

        final ConnectorHandleResolver connectorHandleResolver = connector.getHandleResolver();
        Preconditions
            .checkNotNull(connectorHandleResolver, "Connector %s does not have a handle resolver", connectorId);

        final List<PropertyMetadata<?>> tableProperties = connector.getTableProperties();
        Preconditions.checkNotNull(tableProperties, "Connector %s returned null table properties", connectorId);

        // IMPORTANT: all the instances need to be fetched from the connector *before* we add them to the corresponding
        // managers.Otherwise, a broken connector would leave the managers in an inconsistent state with
        // respect to each other

        metadataManager.addConnectorMetadata(connectorId, catalogName, connectorMetadata);
        splitManager.addConnectorSplitManager(connectorId, connectorSplitManager);
        handleResolver.addHandleResolver(connectorId, connectorHandleResolver);
        metadataManager.getSessionPropertyManager()
            .addConnectorSessionProperties(catalogName, connector.getSessionProperties());
        metadataManager.getTablePropertyManager().addTableProperties(catalogName, tableProperties);
    }

    private static String getConnectorId(final String catalogName) {
        // for now connectorId == catalogName
        return catalogName;
    }
}
