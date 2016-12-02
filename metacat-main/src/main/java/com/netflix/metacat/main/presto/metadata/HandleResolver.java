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
package com.netflix.metacat.main.presto.metadata;

import com.facebook.presto.metadata.LegacyTableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.google.common.base.Preconditions;

import javax.inject.Inject;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Handle resolver.
 */
public class HandleResolver {
    private final ConcurrentMap<String, ConnectorHandleResolver> handleIdResolvers = new ConcurrentHashMap<>();

    /**
     * Default constructor.
     */
    public HandleResolver() {
    }

    /**
     * Constructor.
     * @param handleIdResolvers resolvers
     */
    @Inject
    public HandleResolver(final Map<String, ConnectorHandleResolver> handleIdResolvers) {
        this.handleIdResolvers.putAll(handleIdResolvers);
    }

    /**
     * Add resolver.
     * @param id connector id
     * @param connectorHandleResolver resolver
     */
    public void addHandleResolver(final String id, final ConnectorHandleResolver connectorHandleResolver) {
        final ConnectorHandleResolver existingResolver = handleIdResolvers.putIfAbsent(id, connectorHandleResolver);
        Preconditions.
            checkState(existingResolver == null, "Id %s is already assigned to resolver %s", id, existingResolver);
    }

    /**
     * Get the connector.
     * @param tableHandle table handle
     * @return connector id
     */
    public String getId(final ConnectorTableHandle tableHandle) {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(tableHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for table handle: " + tableHandle);
    }

    /**
     * Get the connector.
     * @param handle table handle
     * @return connector id
     */
    public String getId(final ConnectorTableLayoutHandle handle) {
        if (handle instanceof LegacyTableLayoutHandle) {
            final LegacyTableLayoutHandle legacyHandle = (LegacyTableLayoutHandle) handle;
            for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
                if (entry.getValue().canHandle(legacyHandle.getTable())) {
                    return entry.getKey();
                }
            }
        } else {
            for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
                if (entry.getValue().canHandle(handle)) {
                    return entry.getKey();
                }
            }
        }
        throw new IllegalArgumentException("No connector for table handle: " + handle);
    }

    /**
     * Get the connector.
     * @param  columnHandle column Handle
     * @return connector id
     */
    public String getId(final ColumnHandle columnHandle) {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(columnHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for column handle: " + columnHandle);
    }

    /**
     * Get the connector.
     * @param  split split
     * @return connector id
     */
    public String getId(final ConnectorSplit split) {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(split)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for split: " + split);
    }

    /**
     * Get the connector.
     * @param  indexHandle handle
     * @return connector id
     */
    public String getId(final ConnectorIndexHandle indexHandle) {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(indexHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for index handle: " + indexHandle);
    }

    /**
     * Get the connector.
     * @param  outputHandle handle
     * @return connector id
     */
    public String getId(final ConnectorOutputTableHandle outputHandle) {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(outputHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for output table handle: " + outputHandle);
    }

    /**
     * Get the connector.
     * @param  insertHandle handle
     * @return connector id
     */
    public String getId(final ConnectorInsertTableHandle insertHandle) {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(insertHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for insert table handle: " + insertHandle);
    }

    /**
     * Get handle class.
     * @param id connector id
     * @return handle class
     */
    public Class<? extends ConnectorTableHandle> getTableHandleClass(final String id) {
        return resolverFor(id).getTableHandleClass();
    }

    /**
     * Get handle class.
     * @param id connector id
     * @return handle class
     */
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass(final String id) {
        try {
            return resolverFor(id).getTableLayoutHandleClass();
        } catch (UnsupportedOperationException e) {
            return LegacyTableLayoutHandle.class;
        }
    }

    /**
     * Get handle class.
     * @param id connector id
     * @return handle class
     */
    public Class<? extends ColumnHandle> getColumnHandleClass(final String id) {
        return resolverFor(id).getColumnHandleClass();
    }

    /**
     * Get handle class.
     * @param id connector id
     * @return handle class
     */
    public Class<? extends ConnectorSplit> getSplitClass(final String id) {
        return resolverFor(id).getSplitClass();
    }

    /**
     * Get handle class.
     * @param id connector id
     * @return handle class
     */
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass(final String id) {
        return resolverFor(id).getIndexHandleClass();
    }

    /**
     * Get handle class.
     * @param id connector id
     * @return handle class
     */
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass(final String id) {
        return resolverFor(id).getOutputTableHandleClass();
    }

    /**
     * Get handle class.
     * @param id connector id
     * @return handle class
     */
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass(final String id) {
        return resolverFor(id).getInsertTableHandleClass();
    }

    /**
     * Get resolver.
     * @param id connector id
     * @return resolver
     */
    public ConnectorHandleResolver resolverFor(final String id) {
        final ConnectorHandleResolver resolver = handleIdResolvers.get(id);
        Preconditions.checkArgument(resolver != null, "No handle resolver for %s", id);
        return resolver;
    }

    // *********************
    //
    // NETFLIX addition
    //
    // **********************

    /**
     * Flush the given catalog.
     * @param catalogName catalog name
     */
    public synchronized void flush(final String catalogName) {
        handleIdResolvers.remove(catalogName);
    }

    /**
     * Flush all catalogs.
     */
    public synchronized void flushAll() {
        handleIdResolvers.clear();
    }
}
