package com.netflix.metacat.connector.hive.iceberg;

import com.netflix.metacat.common.server.properties.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

/**
 * Implemented the BaseMetastoreTableOperations to interact with iceberg library.
 * Read only operations.
 */
public class IcebergTableOps extends BaseMetastoreTableOperations {
    private String location;
    private final Configuration conf;
    private final Config config;
    private final IcebergTableOpsProxy icebergTableOpsProxy;
    private TableMetadata tableMetadata;

    /**
     * Constructor.
     * @param conf hive configuration
     * @param location table manifest location
     * @param config server config
     * @param icebergTableOpsProxy IcebergTableOps proxy
     */
    public IcebergTableOps(final Configuration conf,
                           final String location,
                           final Config config,
                           final IcebergTableOpsProxy icebergTableOpsProxy) {
        this.location = location;
        this.conf = conf;
        this.config = config;
        this.icebergTableOpsProxy = icebergTableOpsProxy;
    }

    @Override
    public TableMetadata current() {
        if (tableMetadata == null) {
            tableMetadata = icebergTableOpsProxy.getMetadata(this, config.isIcebergCacheEnabled());
        }
        return tableMetadata;
    }

    @Override
    public FileIO io() {
        return new HadoopFileIO(conf);
    }

    @Override
    public TableMetadata refresh() {
        refreshFromMetadataLocation(this.location, config.getIcebergRefreshFromMetadataLocationRetryNumber());
        return super.current();
    }

    @Override
    public String currentMetadataLocation() {
        return location;
    }

    @Override
    public void commit(final TableMetadata base, final TableMetadata metadata) {
        if (!base.equals(metadata)) {
            location = writeNewMetadata(metadata, currentVersion() + 1);
            tableMetadata = null;
            this.requestRefresh();
        }
    }
}
