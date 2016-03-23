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
import com.facebook.presto.metadata.LegacyTableLayoutHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitDetailManager;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.SavePartitionResult;
import com.facebook.presto.spi.SchemaTablePartitionName;
import com.facebook.presto.spi.Sort;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.split.ConnectorAwareSplitSource;
import com.facebook.presto.split.SplitSource;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class SplitManager extends com.facebook.presto.split.SplitManager
{
    private final ConcurrentMap<String, ConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();

    public void addConnectorSplitManager(String connectorId, ConnectorSplitManager connectorSplitManager)
    {
        checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", connectorId);
    }

    public SplitSource getSplits(Session session, TableLayoutHandle layout)
    {
        String connectorId = layout.getConnectorId();
        ConnectorSplitManager splitManager = getConnectorSplitManager(connectorId);

        // assumes connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(connectorId);

        ConnectorSplitSource source;
        if (layout.getConnectorHandle() instanceof LegacyTableLayoutHandle) {
            LegacyTableLayoutHandle handle = (LegacyTableLayoutHandle) layout.getConnectorHandle();
            if (handle.getPartitions().isEmpty()) {
                return new ConnectorAwareSplitSource(connectorId, new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of()));
            }

            source = splitManager.getPartitionSplits(connectorSession, handle.getTable(), handle.getPartitions());
        }
        else {
            source = splitManager.getSplits(connectorSession, layout.getConnectorHandle());
        }

        return new ConnectorAwareSplitSource(connectorId, source);
    }

    public ConnectorSplitManager getConnectorSplitManager(String connectorId)
    {
        ConnectorSplitManager result = splitManagers.get(connectorId);
        checkArgument(result != null, "No split manager for connector '%s'", connectorId);

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////////////////

    /**
     * NETFLIX addition
     */
    public synchronized void flush(String catalogName){
        splitManagers.remove(catalogName);
    }

    /**
     * NETFLIX addition
     */
    public synchronized void flushAll(){
        splitManagers.clear();
    }

    public SavePartitionResult savePartitions(TableHandle table, List<ConnectorPartition> partitions
            , List<String> partitionIdsForDeletes, boolean checkIfExists){
        ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if( splitManager instanceof ConnectorSplitDetailManager){
            return ((ConnectorSplitDetailManager) splitManager).savePartitions(table.getConnectorHandle(), partitions, partitionIdsForDeletes, checkIfExists);
        } else {
            throw new UnsupportedOperationException("Operation not supported");
        }
    }

    public ConnectorPartitionResult getPartitions(TableHandle table, String filter, List<String> partitionNames, Sort sort, Pageable pageable, boolean includePartitionDetails){
        ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if( splitManager instanceof ConnectorSplitDetailManager){
            return ((ConnectorSplitDetailManager) splitManager).getPartitions( table.getConnectorHandle(), filter, partitionNames, sort, pageable, includePartitionDetails);
        } else {
            throw new UnsupportedOperationException("Operation not supported");
        }
    }

    public Integer getPartitionCount(Session session, TableHandle table){
        ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if( splitManager instanceof ConnectorSplitDetailManager){
            return ((ConnectorSplitDetailManager) splitManager).getPartitionCount(table.getConnectorHandle());
        } else {
            return splitManager.getPartitions(session.toConnectorSession(), table.getConnectorHandle(), TupleDomain.<ColumnHandle>all()).getPartitions().size();
        }
    }

    public void deletePartitions(TableHandle table, List<String> partitionIds){
        ConnectorSplitManager splitManager = getConnectorSplitManager(table.getConnectorId());
        if( splitManager instanceof ConnectorSplitDetailManager){
            ((ConnectorSplitDetailManager) splitManager).deletePartitions( table.getConnectorHandle(), partitionIds);
        } else {
            throw new UnsupportedOperationException("Operation not supported");
        }
    }

    public List<SchemaTablePartitionName> getPartitionNames(Session session, String uri, boolean prefixSearch){
        ConnectorSplitManager splitManager = getConnectorSplitManager(session.getCatalog());
        if( splitManager instanceof ConnectorSplitDetailManager){
            ConnectorSplitDetailManager splitDetailManager = (ConnectorSplitDetailManager) splitManager;
            return splitDetailManager.getPartitionNames(uri, prefixSearch);
        }
        return Lists.newArrayList();
    }
}
