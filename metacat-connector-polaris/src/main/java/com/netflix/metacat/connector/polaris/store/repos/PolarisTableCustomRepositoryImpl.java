package com.netflix.metacat.connector.polaris.store.repos;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;

import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Implementation for Custom JPA repository implementation for storing PolarisTableEntity.
 */
@Repository
public class PolarisTableCustomRepositoryImpl extends BasePolarisCustomRepository
    implements PolarisTableCustomRepository {

    /**
     * Initialize {@link PolarisTableCustomRepositoryImpl}.
     *
     * @param defaultEntityManager - defaultEntityManager
     * @param readerEntityManager  - readerEntityManager
     */
    @Autowired
    public PolarisTableCustomRepositoryImpl(
        @Qualifier("entityManager") final EntityManager defaultEntityManager, // Ensures use of primary EntityManager
        @Qualifier("readEntityManager") final Optional<EntityManager> readerEntityManager) {
        super(defaultEntityManager, readerEntityManager);
    }

    private <T> Slice<T> findAllTablesByDbNameAndTablePrefixForCurrentPage(
        final String dbName,
        final String tableNamePrefix,
        final Pageable page,
        final boolean selectAllColumns) {

        // Generate ORDER BY clause
        String orderBy = "";
        if (page.getSort().isSorted()) {
            orderBy = page.getSort().stream()
                .map(order -> order.getProperty() + " " + order.getDirection())
                .collect(Collectors.joining(", "));
            orderBy = " ORDER BY " + orderBy;
        }

        final String selectClause = selectAllColumns ? "t.*" : "t.tbl_name";
        final String sql = "SELECT " + selectClause + " FROM TBLS t "
            + "WHERE t.db_name = :dbName AND t.tbl_name LIKE :tableNamePrefix" + orderBy;

        Query query;
        if (selectAllColumns) {
            query = getEntityManager().createNativeQuery(sql, PolarisTableEntity.class);
        } else {
            query = getEntityManager().createNativeQuery(sql);
        }
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

    @Override
    @Transactional
    public List<?> findAllTablesByDbNameAndTablePrefix(
        final String dbName,
        final String tableNamePrefix,
        final int pageFetchSize,
        final boolean selectAllColumns,
        final boolean isAuroraEnabled) {
        Pageable page = PageRequest.of(0, pageFetchSize, Sort.by("tbl_name").ascending());
        if (!isAuroraEnabled) {
            getEntityManager().createNativeQuery("SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()")
                .executeUpdate();
        }
        final List<Object> retval = new ArrayList<>();
        final String tblPrefix =  tableNamePrefix == null ? "" : tableNamePrefix;
        Slice<?> tbls;
        boolean hasNext;
        do {
            tbls = findAllTablesByDbNameAndTablePrefixForCurrentPage(dbName, tblPrefix, page, selectAllColumns);
            retval.addAll(tbls.getContent());
            hasNext = tbls.hasNext();
            if (hasNext) {
                page = tbls.nextPageable();
            }
        } while (hasNext);
        return retval;
    }
}
