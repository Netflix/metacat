package com.netflix.metacat.connector.polaris.store.repos;

import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.EntityTransaction;

import com.netflix.metacat.connector.polaris.store.entities.PolarisTableEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation for Custom JPA repository implementation for storing PolarisTableEntity.
 */
@Repository
public class PolarisTableCustomRepositoryImpl implements PolarisTableCustomRepository {
    @Autowired
    private EntityManagerFactory entityManagerFactory;

    private Slice<PolarisTableEntity> findAllTablesByDbNameAndTablePrefixForCurrentPage(
        final String dbName, final String tableNamePrefix, final Pageable page, final EntityManager entityManager) {

        final String sql = "SELECT t.* FROM TBLS t "
            + "WHERE t.db_name = :dbName AND t.tbl_name LIKE :tableNamePrefix";

        final Query query = entityManager.createNativeQuery(sql, PolarisTableEntity.class);
        query.setParameter("dbName", dbName);
        query.setParameter("tableNamePrefix", tableNamePrefix + "%");

        query.setFirstResult(page.getPageNumber() * page.getPageSize());
        query.setMaxResults(page.getPageSize());

        final List<PolarisTableEntity> resultList = query.getResultList();

        // Check if there is a next page
        final boolean hasNext = resultList.size() > page.getPageSize();

        // If there is a next page, remove the last item from the list
        if (hasNext) {
            resultList.remove(resultList.size() - 1);
        }

        return new SliceImpl<>(resultList, page, hasNext);
    }


    @SuppressWarnings("checkstyle:FinalParameters")
    @Override
    public List<PolarisTableEntity> findAllTablesByDbNameAndTablePrefix(
        final String dbName, final String tableNamePrefix) {
        final int pageFetchSize = 1000;
        Pageable page = PageRequest.of(0, pageFetchSize, Sort.by("tbl_name").ascending());

        final EntityManager entityManager = entityManagerFactory.createEntityManager();
        final EntityTransaction transaction = entityManager.getTransaction();

        try {
            transaction.begin();

            // Execute your SQL statement
            entityManager.createNativeQuery("SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()")
                .executeUpdate();

            final List<PolarisTableEntity> retval = new ArrayList<>();
            final String tblPrefix =  tableNamePrefix == null ? "" : tableNamePrefix;
            Slice<PolarisTableEntity> tbls;
            boolean hasNext;

            do {
                tbls = findAllTablesByDbNameAndTablePrefixForCurrentPage(dbName, tblPrefix, page, entityManager);
                retval.addAll(tbls.toList());
                hasNext = tbls.hasNext();
                if (hasNext) {
                    page = tbls.nextPageable();
                }
            } while (hasNext);

            transaction.commit();

            return retval;
        } catch (Exception e) {
            // If there's an exception, roll back the transaction
            if (transaction.isActive()) {
                transaction.rollback();
            }
            // Handle or rethrow the exception
            throw e;
        } finally {
            entityManager.close();
        }
    }
}
