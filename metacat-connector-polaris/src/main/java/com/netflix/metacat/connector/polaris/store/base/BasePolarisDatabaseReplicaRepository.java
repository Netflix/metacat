package com.netflix.metacat.connector.polaris.store.base;

import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.connector.polaris.store.repos.PolarisDatabaseCustomReplicaRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for Polaris database replica repositories.
 * Provides common methods for retrieving database information.
 */
public abstract class BasePolarisDatabaseReplicaRepository
    implements PolarisDatabaseCustomReplicaRepository {

    /**
     * Retrieves a slice of databases for the current page.
     *
     * @param dbNamePrefix the prefix of the database name to filter results
     * @param page the pageable object containing pagination information
     * @param selectAllColumns flag indicating whether to select all columns or just specific ones
     * @param <T> the type of the database entity
     * @return a Slice containing the databases for the current page
     */
    protected <T> Slice<T> getAllDatabasesForCurrentPage(
        final String dbNamePrefix, final Pageable page, final boolean selectAllColumns) {
        return null;
    }

    /**
     * Retrieves all databases with optional filtering and sorting.
     *
     * @param dbNamePrefix the prefix of the database name to filter results, can be null
     * @param sort the sort order and field, can be null
     * @param pageSize the size of each page
     * @param selectAllColumns flag indicating whether to select all columns or just specific ones
     * @return a list of all databases matching the criteria
     */
    @Override
    public List<?> getAllDatabases(
        @Nullable final String dbNamePrefix,
        @Nullable final com.netflix.metacat.common.dto.Sort sort,
        final int pageSize,
        final boolean selectAllColumns) {
        final List<Object> retval = new ArrayList<>();

        final String dbPrefix = dbNamePrefix == null ? "" : dbNamePrefix;

        // By default, sort name in ascending order
        Sort dbSort = Sort.by("name").ascending();
        if (sort != null && sort.hasSort()) {
            if (sort.getOrder() == SortOrder.DESC) {
                dbSort = Sort.by(sort.getSortBy()).descending();
            } else {
                dbSort = Sort.by(sort.getSortBy()).ascending();
            }
        }

        Pageable page = PageRequest.of(0, pageSize, dbSort);
        Slice<?> dbs;
        boolean hasNext;
        do {
            dbs = getAllDatabasesForCurrentPage(dbPrefix, page, selectAllColumns);
            retval.addAll(dbs.getContent());
            hasNext = dbs.hasNext();
            if (hasNext) {
                page = dbs.nextPageable();
            }
        } while (hasNext);
        return retval;
    }
}
