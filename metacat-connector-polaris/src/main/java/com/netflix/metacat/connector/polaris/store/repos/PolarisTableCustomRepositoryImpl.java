package com.netflix.metacat.connector.polaris.store.repos;

import javax.persistence.PersistenceContext;
import javax.persistence.EntityManager;
import javax.persistence.Query;

import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation for Custom JPA repository implementation for storing PolarisTableEntity.
 */
@Repository
public class PolarisTableCustomRepositoryImpl implements PolarisTableCustomRepository {
    @PersistenceContext
    private EntityManager entityManager;

    private Slice<PolarisTableEntity> findAllTablesByDbNameAndTablePrefixForCurrentPage(
        final String dbName, final String tableNamePrefix, final Pageable page) {

        // Generate ORDER BY clause
        String orderBy = "";
        if (page.getSort().isSorted()) {
            orderBy = page.getSort().stream()
                .map(order -> order.getProperty() + " " + order.getDirection())
                .collect(Collectors.joining(", "));
            orderBy = " ORDER BY " + orderBy;
        }

        final String sql = "SELECT t.* FROM TBLS t "
            + "WHERE t.db_name = :dbName AND t.tbl_name LIKE :tableNamePrefix" + orderBy;
        final Query query = entityManager.createNativeQuery(sql, PolarisTableEntity.class);
        query.setParameter("dbName", dbName);
        query.setParameter("tableNamePrefix", tableNamePrefix + "%");
        query.setFirstResult(page.getPageNumber() * page.getPageSize());
        query.setMaxResults(page.getPageSize() + 1); // Fetch one extra result to determine if there is a next page
        final List<PolarisTableEntity> resultList = query.getResultList();
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
    public List<PolarisTableEntity> findAllTablesByDbNameAndTablePrefix(
        final String dbName, final String tableNamePrefix, final int pageFetchSize) {
        Pageable page = PageRequest.of(0, pageFetchSize, Sort.by("tbl_name").ascending());
        entityManager.createNativeQuery("SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()")
            .executeUpdate();
        final List<PolarisTableEntity> retval = new ArrayList<>();
        final String tblPrefix =  tableNamePrefix == null ? "" : tableNamePrefix;
        Slice<PolarisTableEntity> tbls;
        boolean hasNext;
        do {
            tbls = findAllTablesByDbNameAndTablePrefixForCurrentPage(dbName, tblPrefix, page);
            retval.addAll(tbls.toList());
            hasNext = tbls.hasNext();
            if (hasNext) {
                page = tbls.nextPageable();
            }
        } while (hasNext);
        return retval;
    }
}
