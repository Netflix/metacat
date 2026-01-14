package com.netflix.metacat.common.server.connector.iceberg;

import org.apache.iceberg.TableMetadata;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;

/**
 * Proxy class to get the metadata from cache if exists.
 */
@CacheConfig(cacheNames = "metacat")
public class IcebergTableOpsProxy {
    /**
     * Return the table metadata from cache if exists. If not exists, make the iceberg call to refresh it.
     * @param icebergTableOps iceberg table operations
     * @param useCache true, if table can be retrieved from cache
     * @return TableMetadata
     */
    @Cacheable(
        cacheNames = "metacat",
        key = "'iceberg.' + #icebergTableOps.currentMetadataLocation()",
        condition = "#useCache"
    )
    public TableMetadata getMetadata(final IcebergTableOps icebergTableOps, final boolean useCache) {
        return icebergTableOps.refresh();
    }
}
