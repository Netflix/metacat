package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.common.dto.SortOrder;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nullable;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation for Custom JPA repository implementation for interacting with PolarisDatabaseEntity.
 */
public class PolarisDatabaseCustomRepositoryImpl implements PolarisDatabaseCustomRepository {
    @PersistenceContext
    private EntityManager entityManager;

    private <T> Slice<T> getAllDatabasesForCurrentPage(
        final String dbNamePrefix, final Pageable page, final boolean selectAllColumns) {

        // Generate ORDER BY clause
        String orderBy = "";
        if (page.getSort().isSorted()) {
            orderBy = page.getSort().stream()
                .map(order -> order.getProperty() + " " + order.getDirection())
                .collect(Collectors.joining(", "));
            orderBy = " ORDER BY " + orderBy;
        }

        final String selectClause = selectAllColumns ? "d.*" : "d.name";
        final String sql = "SELECT " + selectClause + " FROM DBS d "
            + "WHERE d.name LIKE :dbNamePrefix" + orderBy;

        Query query;
        if (selectAllColumns) {
            query = entityManager.createNativeQuery(sql, PolarisDatabaseEntity.class);
        } else {
            query = entityManager.createNativeQuery(sql);
        }
        query.setParameter("dbNamePrefix", dbNamePrefix + "%");
        query.setFirstResult(page.getPageNumber() * page.getPageSize());
        query.setMaxResults(page.getPageSize() + 1); // Fetch one extra result to determine if there is a next page
        final List<T> resultList = query.getResultList();

        // Check if there is a next page
        final boolean hasNext = resultList.size() > page.getPageSize();

        // If there is a next page, remove the last item from the list
        if (hasNext) {
            resultList.remove(resultList.size() - 1);
        }
        return new SliceImpl<>(resultList, page, hasNext);
    }

    @Override
    @Transactional
    public List<?> getAllDatabases(
        @Nullable final String dbNamePrefix,
        @Nullable final com.netflix.metacat.common.dto.Sort sort,
        final int pageSize,
        final boolean selectAllColumns) {
        final List<Object> retval = new ArrayList<>();

        final String dbPrefix =  dbNamePrefix == null ? "" : dbNamePrefix;

        // by default sort name in ascending order
        Sort dbSort = Sort.by("name").ascending();
        if (sort != null && sort.hasSort()) {
            if (sort.getOrder() == SortOrder.DESC) {
                dbSort = Sort.by(sort.getSortBy()).descending();
            } else {
                dbSort = Sort.by(sort.getSortBy()).ascending();
            }
        }

        Pageable page = PageRequest.of(0, pageSize, dbSort);
        /*entityManager.createNativeQuery("SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()")
            .executeUpdate();*/
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
