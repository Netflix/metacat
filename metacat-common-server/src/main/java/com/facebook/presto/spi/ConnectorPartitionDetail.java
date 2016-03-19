package com.facebook.presto.spi;

import java.util.Map;

/**
 * Created by amajumdar on 2/2/15.
 */
public interface ConnectorPartitionDetail extends ConnectorPartition
{
    /**
     * Gets any extra properties of a partition that is relevant to a particular catalog.
     * @return extra properties other than the partition key
     */
    Map<String, String> getMetadata();

    /**
     * Gets the storage related information about the partition. This applies mostly in the case of unstructured data stored as files.
     * @return storage information related properties
     */
    StorageInfo getStorageInfo();

    /**
     * Gets the audit information like created date, last update date etc..
     * @return audit information
     */
    AuditInfo getAuditInfo();
}
