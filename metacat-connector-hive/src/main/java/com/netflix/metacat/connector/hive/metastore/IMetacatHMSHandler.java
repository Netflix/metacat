package com.netflix.metacat.connector.hive.metastore;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;

import java.util.List;

/**
 * IMetacatHMSHandler.
 * @author zhenl
 * @since 1.0.0
 */
public interface IMetacatHMSHandler extends IHMSHandler {
    /**
     * Adds and drops partitions in one transaction.
     *
     * @param databaseName database name
     * @param tableName    table name
     * @param addParts     list of partitions
     * @param dropParts    list of partition values
     * @param deleteData   if true, deletes the data
     * @return true if successful
     * @throws TException   any internal exception
     */
    @SuppressWarnings({"checkstyle:methodname"})
    boolean add_drop_partitions(String databaseName,
                                String tableName, List<Partition> addParts,
                                List<List<String>> dropParts, boolean deleteData)
            throws TException;
}
