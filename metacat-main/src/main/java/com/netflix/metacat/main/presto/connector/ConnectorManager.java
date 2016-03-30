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
import com.netflix.metacat.main.presto.metadata.HandleResolver;
import com.netflix.metacat.main.presto.metadata.MetadataManager;
import com.netflix.metacat.main.presto.split.SplitManager;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ConnectorManager
{
    private static final Logger log = Logger.get(ConnectorManager.class);

    private final MetadataManager metadataManager;
    private final SplitManager splitManager;
    private final HandleResolver handleResolver;

    private final ConcurrentMap<String, ConnectorFactory> connectorFactories = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Connector> connectors = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();

    @Inject
    public ConnectorManager(MetadataManager metadataManager,
            SplitManager splitManager,
            HandleResolver handleResolver,
            Map<String, ConnectorFactory> connectorFactories)
    {
        this.metadataManager = metadataManager;
        this.splitManager = splitManager;
        this.handleResolver = handleResolver;
        this.connectorFactories.putAll(connectorFactories);
    }

    @PreDestroy
    public void stop()
    {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (Map.Entry<String, Connector> entry : connectors.entrySet()) {
            Connector connector = entry.getValue();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connector.getClass().getClassLoader())) {
                connector.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down connector: %s", entry.getKey());
            }
        }
    }

    public void addConnectorFactory(ConnectorFactory connectorFactory)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        ConnectorFactory existingConnectorFactory = connectorFactories.putIfAbsent(connectorFactory.getName(), connectorFactory);
        checkArgument(existingConnectorFactory == null, "Connector %s is already registered", connectorFactory.getName());
    }

    public synchronized void createConnection(String catalogName, String connectorName, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(connectorName, "connectorName is null");
        checkNotNull(properties, "properties is null");

        ConnectorFactory connectorFactory = connectorFactories.get(connectorName);
        checkArgument(connectorFactory != null, "No factory for connector %s", connectorName);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(connectorFactory.getClass().getClassLoader())) {
            createConnection(catalogName, connectorFactory, properties);
        }
    }

    public synchronized void createConnection(String catalogName, ConnectorFactory connectorFactory, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(properties, "properties is null");
        checkNotNull(connectorFactory, "connectorFactory is null");

        String connectorId = getConnectorId(catalogName);
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);

        Connector connector = connectorFactory.create(connectorId, properties);

        addConnector(catalogName, connectorId, connector);
    }

    private synchronized void addConnector(String catalogName, String connectorId, Connector connector)
    {
        checkState(!stopped.get(), "ConnectorManager is stopped");
        checkState(!connectors.containsKey(connectorId), "A connector %s already exists", connectorId);
        connectors.put(connectorId, connector);

        ConnectorMetadata connectorMetadata = connector.getMetadata();
        checkState(connectorMetadata != null, "Connector %s can not provide metadata", connectorId);

        ConnectorSplitManager connectorSplitManager = connector.getSplitManager();
        checkState(connectorSplitManager != null, "Connector %s does not have a split manager", connectorId);

        ConnectorHandleResolver connectorHandleResolver = connector.getHandleResolver();
        checkNotNull(connectorHandleResolver, "Connector %s does not have a handle resolver", connectorId);

        List<PropertyMetadata<?>> tableProperties = connector.getTableProperties();
        checkNotNull(tableProperties, "Connector %s returned null table properties", connectorId);

        // IMPORTANT: all the instances need to be fetched from the connector *before* we add them to the corresponding managers.
        // Otherwise, a broken connector would leave the managers in an inconsistent state with respect to each other

        metadataManager.addConnectorMetadata(connectorId, catalogName, connectorMetadata);
        splitManager.addConnectorSplitManager(connectorId, connectorSplitManager);
        handleResolver.addHandleResolver(connectorId, connectorHandleResolver);
        metadataManager.getSessionPropertyManager().addConnectorSessionProperties(catalogName, connector.getSessionProperties());
        metadataManager.getTablePropertyManager().addTableProperties(catalogName, tableProperties);
    }

    private static String getConnectorId(String catalogName)
    {
        // for now connectorId == catalogName
        return catalogName;
    }
}
