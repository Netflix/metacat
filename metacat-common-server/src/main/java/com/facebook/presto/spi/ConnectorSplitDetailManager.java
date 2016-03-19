package com.facebook.presto.spi;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by amajumdar on 2/2/15.
 */
public interface ConnectorSplitDetailManager extends ConnectorSplitManager{
    /**
     * Gets the Partitions based on a filter expression for the specified table.
     * @param table table handle
     * @param filterExpression JSP based filter expression string
     * @param partitionNames filter the list that matches the given partition names. If null or empty, it will return all.
     * @return filtered list of partitions
     */
    ConnectorPartitionResult getPartitions(ConnectorTableHandle table, String filterExpression, List<String> partitionNames, Sort sort, Pageable pageable, boolean includePartitionDetails);

    /**
     * Add/Update/delete partitions for a table
     * @param table table handle
     * @param partitions list of partitions
     * @param partitionIdsForDeletes list of partition ids/names for deletes
     * @return added/updated list of partition names
     */
    SavePartitionResult savePartitions(ConnectorTableHandle table, List<ConnectorPartition> partitions, List<String> partitionIdsForDeletes, boolean checkIfExists);

    /**
     * Delete partitions for a table
     * @param table table handle
     * @param partitionIds list of partition names
     */
    void deletePartitions(ConnectorTableHandle table, List<String> partitionIds);

    /**
     * Number of partitions for the given table
     * @param connectorHandle table handle
     * @return Number of partitions
     */
    Integer getPartitionCount(ConnectorTableHandle connectorHandle);

    /**
     * Returns all the partition names referring to the given <code>uri</code>
     * @param uri location
     * @param prefixSearch if tru, we look for tables whose location starts with the given <code>uri</code>
     * @return list of partition names
     */
    default List<SchemaTablePartitionName> getPartitionNames(String uri, boolean prefixSearch){
        return Lists.newArrayList();
    }
}
