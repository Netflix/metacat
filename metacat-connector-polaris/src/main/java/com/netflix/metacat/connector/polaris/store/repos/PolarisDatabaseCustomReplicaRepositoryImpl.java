package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.Utils;
import com.netflix.metacat.connector.polaris.store.entities.PolarisDatabaseEntity;
import com.netflix.metacat.connector.polaris.store.base.BasePolarisDatabaseReplicaRepository;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Implementation for Custom JPA repository implementation for interacting with PolarisDatabaseEntity.
 */
public class PolarisDatabaseCustomReplicaRepositoryImpl extends BasePolarisDatabaseReplicaRepository {
    @PersistenceContext
    private EntityManager entityManager;

    @Override
    protected <T> Slice<T> getAllDatabasesForCurrentPage(
        final String dbNamePrefix, final Pageable page, final boolean selectAllColumns) {

        final String orderBy = Utils.generateOrderBy(page);

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

    /**
     * Retrieves all databases with optional filtering and sorting for crdb.
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
        entityManager.createNativeQuery("SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()")
                .executeUpdate();
        return super.getAllDatabases(dbNamePrefix, sort, pageSize, selectAllColumns);
    }
}
