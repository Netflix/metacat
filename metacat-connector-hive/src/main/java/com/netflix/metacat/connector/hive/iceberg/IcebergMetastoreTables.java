package com.netflix.metacat.connector.hive.iceberg;

import com.netflix.metacat.common.exception.MetacatNotSupportedException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;

/**
 * Implemented BaseMetastoreTables to interact with iceberg library.
 * Load an iceberg table from a location.
 */
public final class IcebergMetastoreTables extends BaseMetastoreCatalog {
    private IcebergTableOps tableOperations;

    IcebergMetastoreTables(final IcebergTableOps tableOperations) {
        this.tableOperations = tableOperations;
    }

    @Override
    public Table createTable(final TableIdentifier identifier,
                             final Schema schema,
                             final PartitionSpec spec,
                             final String location,
                             final Map<String, String> properties) {
        throw new MetacatNotSupportedException("not supported");
    }

    @Override
    public Transaction newCreateTableTransaction(final TableIdentifier identifier,
                                                 final Schema schema,
                                                 final PartitionSpec spec,
                                                 final String location,
                                                 final Map<String, String> properties) {
        throw new MetacatNotSupportedException("not supported");
    }

    @Override
    public Transaction newReplaceTableTransaction(final TableIdentifier identifier,
                                                  final Schema schema,
                                                  final PartitionSpec spec,
                                                  final String location,
                                                  final Map<String, String> properties,
                                                  final boolean orCreate) {
        throw new MetacatNotSupportedException("not supported");
    }

    @Override
    public Table loadTable(final TableIdentifier identifier) {
        return super.loadTable(identifier);
    }

    @Override
    protected TableOperations newTableOps(final TableIdentifier tableIdentifier) {
        return getTableOps();
    }

    @Override
    protected String defaultWarehouseLocation(final TableIdentifier tableIdentifier) {
        throw new MetacatNotSupportedException("not supported");
    }

    @Override
    public boolean dropTable(final TableIdentifier identifier,
                             final boolean purge) {
        throw new MetacatNotSupportedException("not supported");
    }

    @Override
    public void renameTable(final TableIdentifier from,
                            final TableIdentifier to) {
        throw new MetacatNotSupportedException("not supported");
    }

    /**
     * Creates a new instance of IcebergTableOps for the given table location, if not exists.
     *
     * @return a MetacatServerOps for the table
     */
    public IcebergTableOps getTableOps() {
        return tableOperations;
    }
}
