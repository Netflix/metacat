package com.netflix.metacat.connector.polaris.store.base;

import com.netflix.metacat.connector.polaris.store.repos.PolarisTableCustomReplicaRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;

/**
 * Base repository implementation for handling operations on Polaris table replicas.
 * This class provides methods to retrieve tables by database name and table prefix.
 */
public class BasePolarisTableReplicaRepository
    implements PolarisTableCustomReplicaRepository {

    /**
     * Finds a slice of tables by database name and table prefix for the current page.
     * This method is intended to be overridden by subclasses to provide specific implementation.
     *
     * @param dbName the name of the database
     * @param tableNamePrefix the prefix of the table name to filter results
     * @param page the pageable object containing pagination information
     * @param selectAllColumns flag indicating whether to select all columns or just specific ones
     * @param <T> the type of the elements in the slice
     * @return a Slice of table results, or null if not overridden
     */
    protected <T> Slice<T> findAllTablesByDbNameAndTablePrefixForCurrentPage(
            final String catalogName,
        final String dbName,
        final String tableNamePrefix,
        final Pageable page,
        final boolean selectAllColumns) {
        return null;
    }

    /**
     * Retrieves all tables by database name and table prefix.
     * Fetches tables in pages and collects them into a list.
     *
     * @param dbName the name of the database
     * @param tableNamePrefix the prefix of the table name to filter results
     * @param pageFetchSize the number of records to fetch per page
     * @param selectAllColumns flag indicating whether to select all columns or just specific ones
     * @return a List of table results
     */
    @Override
    public List<?> findAllTablesByDbNameAndTablePrefix(
            final String catalogName,
        final String dbName,
        final String tableNamePrefix,
        final int pageFetchSize,
        final boolean selectAllColumns) {
        Pageable page = PageRequest.of(0, pageFetchSize, Sort.by("tbl_name").ascending());
        final List<Object> retval = new ArrayList<>();
        final String tblPrefix = tableNamePrefix == null ? "" : tableNamePrefix;
        Slice<?> tbls;
        boolean hasNext;
        do {
            tbls = findAllTablesByDbNameAndTablePrefixForCurrentPage(
                    catalogName, dbName, tblPrefix, page, selectAllColumns);
            retval.addAll(tbls.getContent());
            hasNext = tbls.hasNext();
            if (hasNext) {
                page = tbls.nextPageable();
            }
        } while (hasNext);
        return retval;
    }
}
