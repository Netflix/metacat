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
package com.netflix.metacat.main.presto.split;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitDetailManager;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.SavePartitionResult;
import com.facebook.presto.spi.SchemaTablePartitionName;
import com.facebook.presto.spi.Sort;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Split Manager.
 */
public class SplitManager {
    private final ConcurrentMap<String, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();

    /**
     * Adds connector split manager.
     * @param connectorId connector id
     * @param connectorSplitManager manager
     */
    public void addConnectorSplitManager(final String connectorId, final ConnectorSplitManager connectorSplitManager) {
        Preconditions.checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null,
            "SplitManager for connector '%s' is already registered", connectorId);
    }

    /**
     * Gets the split manager.
     * @param connectorId connector id
     * @return split manager
     */
    public ConnectorSplitManager getConnectorSplitManager(final String connectorId) {
        final ConnectorSplitManager result = splitManagers.get(connectorId);
        Preconditions.checkArgument(result != null, "No split manager for connector '%s'", connectorId);

        return result;
    }

    // *********************
    //
    // NETFLIX addition
    //
    // **********************

    /**
     * Flushes the catalog.
     * @param catalogName catalog name.
     */
    public synchronized void flush(final String catalogName) {
        splitManagers.remove(catalogName);
    }

    /**
     * Flushes all catalogs.
     */
    public synchronized void flushAll() {
        splitManagers.clear();
    }

    /**
     * Saves the list of partitions to the given table <code>name</code>. By default, if a partition exists, it drops
     * the partition before adding it. If <code>alterIfExists</code> is true, then it will alter the partition.
     * @param table table handle
     * @param partitions list of partition info
     * @param partitionIdsForDeletes list of partition names to be deleted
     * @param checkIfExists if true, this method checks if any of the partitions exists for the given table
     * @param alterIfExists if true, the method alters the partition instead of dropping and adding the partition
     * @return no. of partitions added and updated.
     */
    public SavePartitionResult savePartitions(final TableHandle table, final List<ConnectorPartition> partitions,
        final List<String> partitionIdsForDeletes, final boolean checkIfExists, final boolean alterIfExists) {
        final ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if (splitManager instanceof ConnectorSplitDetailManager) {
            return ((ConnectorSplitDetailManager) splitManager)
                .savePartitions(table.getConnectorHandle(), partitions, partitionIdsForDeletes,
                    checkIfExists, alterIfExists);
        } else {
            throw new UnsupportedOperationException("Operation not supported");
        }
    }

    /**
     * Returns the list of partitions.
     * @param table table handle
     * @param filter filter expression
     * @param partitionNames partition names to include
     * @param sort sort info
     * @param pageable pagination info
     * @param includePartitionDetails if true, includes parameter details
     * @return list of partitions
     */
    public ConnectorPartitionResult getPartitions(final TableHandle table, final String filter,
        final List<String> partitionNames, final Sort sort, final Pageable pageable,
        final boolean includePartitionDetails) {
        final ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if (splitManager instanceof ConnectorSplitDetailManager) {
            return ((ConnectorSplitDetailManager) splitManager)
                .getPartitions(table.getConnectorHandle(), filter, partitionNames, sort, pageable,
                    includePartitionDetails);
        } else {
            throw new UnsupportedOperationException("Operation not supported");
        }
    }

    /**
     * Returns a list of partition names.
     * @param table table handle
     * @param filter filter expression
     * @param partitionNames names
     * @param sort sort info
     * @param pageable pagination info
     * @return list of partition names
     */
    public List<String> getPartitionKeys(final TableHandle table, final String filter,
        final List<String> partitionNames, final Sort sort, final Pageable pageable) {
        final ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if (splitManager instanceof ConnectorSplitDetailManager) {
            return ((ConnectorSplitDetailManager) splitManager)
                .getPartitionKeys(table.getConnectorHandle(), filter, partitionNames, sort, pageable);
        } else {
            throw new UnsupportedOperationException("Operation not supported");
        }
    }

    /**
     * Returns a list of partition uris.
     * @param table table handle
     * @param filter filter expression
     * @param partitionNames names
     * @param sort sort info
     * @param pageable pagination info
     * @return list of partition uris
     */
    public List<String> getPartitionUris(final TableHandle table, final String filter,
        final List<String> partitionNames, final Sort sort, final Pageable pageable) {
        final ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if (splitManager instanceof ConnectorSplitDetailManager) {
            return ((ConnectorSplitDetailManager) splitManager)
                .getPartitionUris(table.getConnectorHandle(), filter, partitionNames, sort, pageable);
        } else {
            throw new UnsupportedOperationException("Operation not supported");
        }
    }

    /**
     * Partition count for the given table name.
     * @param session session
     * @param table table handle
     * @return no. of partitions
     */
    public Integer getPartitionCount(final Session session, final TableHandle table) {
        final ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if (splitManager instanceof ConnectorSplitDetailManager) {
            return ((ConnectorSplitDetailManager) splitManager).getPartitionCount(table.getConnectorHandle());
        } else {
            return splitManager.getPartitions(session.toConnectorSession(), table.getConnectorHandle(),
                TupleDomain.<ColumnHandle>all()).getPartitions().size();
        }
    }

    /**
     * Deletes the partitions with the given <code>partitionIds</code> for the given table name.
     * @param table table handle
     * @param partitionIds partition names
     */
    public void deletePartitions(final TableHandle table, final List<String> partitionIds) {
        final ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if (splitManager instanceof ConnectorSplitDetailManager) {
            ((ConnectorSplitDetailManager) splitManager).deletePartitions(table.getConnectorHandle(), partitionIds);
        } else {
            throw new UnsupportedOperationException("Operation not supported");
        }
    }

    /**
     * Returns a map of uri to partition names.
     * @param session session
     * @param uris list of uris
     * @param prefixSearch if true, this method does a prefix search
     * @return a map of uri to partition names
     */
    public Map<String, List<SchemaTablePartitionName>> getPartitionNames(final Session session, final List<String> uris,
        final boolean prefixSearch) {
        final ConnectorSplitManager splitManager = getConnectorSplitManager(session.getCatalog());
        if (splitManager instanceof ConnectorSplitDetailManager) {
            final ConnectorSplitDetailManager splitDetailManager = (ConnectorSplitDetailManager) splitManager;
            return splitDetailManager.getPartitionNames(uris, prefixSearch);
        }
        return Maps.newHashMap();
    }
}
