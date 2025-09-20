package com.netflix.metacat.connector.polaris.store.repos;

import com.netflix.metacat.connector.polaris.store.Utils;
import com.netflix.metacat.connector.polaris.store.base.BasePolarisTableReplicaRepository;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;

import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Implementation for Custom JPA repository implementation for storing PolarisTableEntity.
 */
@Repository
public class PolarisTableCustomReplicaRepositoryImpl
    extends BasePolarisTableReplicaRepository {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    protected  <T> Slice<T> findAllTablesByDbNameAndTablePrefixForCurrentPage(
        final String catalogName,
        final String dbName, final String tableNamePrefix, final Pageable page, final boolean selectAllColumns) {

        // Generate ORDER BY clause
        final String orderBy = Utils.generateOrderBy(page);

        final String selectClause = selectAllColumns ? "t.*" : "t.tbl_name";
        final String sql = "SELECT " + selectClause + " FROM TBLS t "
            + "WHERE t.catalog_name = :catalogName AND t.db_name = :dbName AND t.tbl_name LIKE :tableNamePrefix"
                + orderBy;

        Query query;
        if (selectAllColumns) {
            query = entityManager.createNativeQuery(sql, PolarisTableEntity.class);
        } else {
            query = entityManager.createNativeQuery(sql);
        }
        query.setParameter("catalogName", catalogName);
        query.setParameter("dbName", dbName);
        query.setParameter("tableNamePrefix", tableNamePrefix + "%");
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
     * Retrieves all tables by database name and table prefix using crdb.
     * Fetches tables in pages and collects them into a list.
     *
     * @param catalogName catalogName
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
        entityManager.createNativeQuery("SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()")
                .executeUpdate();
        return super.findAllTablesByDbNameAndTablePrefix(
                catalogName,
                dbName,
                tableNamePrefix,
                pageFetchSize,
                selectAllColumns
        );
    }
}
