package com.netflix.metacat.connector.hive.iceberg;

import com.google.common.base.Throwables;
import com.netflix.metacat.common.server.properties.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

/**
 * Implemented the BaseMetastoreTableOperations to interact with iceberg library.
 * Read only operations.
 */
public class IcebergTableOps extends BaseMetastoreTableOperations {
    private String location;
    private String tableName;
    private final Configuration conf;
    private final Config config;
    private final IcebergTableOpsProxy icebergTableOpsProxy;
    private TableMetadata tableMetadata;

    /**
     * Constructor.
     * @param conf hive configuration
     * @param location table manifest location
     * @param tableName table name
     * @param config server config
     * @param icebergTableOpsProxy IcebergTableOps proxy
     */
    public IcebergTableOps(final Configuration conf,
                           final String location,
                           final String tableName,
                           final Config config,
                           final IcebergTableOpsProxy icebergTableOpsProxy) {
        this.location = location;
        this.tableName = tableName;
        this.conf = conf;
        this.config = config;
        this.icebergTableOpsProxy = icebergTableOpsProxy;
    }

    @Override
    protected String tableName() {
        return tableName;
    }

    @Override
    public TableMetadata current() {
        if (tableMetadata == null) {
            tableMetadata =
                icebergTableOpsProxy.getMetadata(this, config.isIcebergTableMetadataCacheEnabled());
        }
        return tableMetadata;
    }

    @Override
    public FileIO io() {
        return new HadoopFileIO(conf);
    }

    @Override
    public TableMetadata refresh() {
        try {
            refreshFromMetadataLocation(this.location, config.getIcebergRefreshFromMetadataLocationRetryNumber());
            return super.current();
        } catch (Exception e) {
            for (Throwable ex : Throwables.getCausalChain(e)) {
                if (ex.getMessage().contains("NoSuchKey")) {
                    throw new NotFoundException(e, String.format("Location %s does not exist", location));
                }
            }
            throw e;
        }
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
