package com.netflix.metacat.s3.connector.dao.impl;

import com.facebook.presto.spi.Pageable;
import com.facebook.presto.spi.Sort;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.netflix.metacat.s3.connector.dao.PartitionDao;
import com.netflix.metacat.s3.connector.model.Partition;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import java.util.List;

/**
 * Created by amajumdar on 1/2/15.
 */
public class PartitionDaoImpl extends IdEntityDaoImpl<Partition> implements PartitionDao {
    private static final String SQL_GET_PARTITIONS = "select * from partition_table as p where p.table_id=:tableId";

    @Inject
    public PartitionDaoImpl(Provider<EntityManager> em) {
        super(em);
    }

    @Override
    protected Class<Partition> getEntityClass() {
        return Partition.class;
    }

    public List<Partition> getPartitions( Long tableId, List<String> partitionIds, Iterable<String> partitionParts, String dateCreatedSqlCriteria, Sort sort, Pageable pageable){
        // Create the sql
        StringBuilder queryBuilder = new StringBuilder(SQL_GET_PARTITIONS);
        if( partitionIds != null && !partitionIds.isEmpty()){
            queryBuilder.append(" and p.name in ('")
                    .append(Joiner.on("','").skipNulls().join(partitionIds))
                    .append("')");
        }
        if( partitionParts != null) {
            for (String singlePartitionExpr : partitionParts) {
                queryBuilder.append(" and p.name like '%").append(singlePartitionExpr).append("%'");
            }
        }
        if( !Strings.isNullOrEmpty(dateCreatedSqlCriteria)){
            queryBuilder.append(" and ").append(dateCreatedSqlCriteria);
        }
        if( sort != null && sort.hasSort()){
            queryBuilder.append(" order by ").append( sort.getSortBy()).append(" ").append(sort.getOrder().name());
        }
        if( pageable != null && pageable.isPageable()){
            queryBuilder.append(" limit ").append( pageable.getOffset()).append(',').append(pageable.getLimit());
        }
        // entityManager
        EntityManager entityManager = em.get();
        Query pQuery = entityManager.createNativeQuery(queryBuilder.toString(), Partition.class);
        pQuery.setParameter("tableId", tableId);
        return pQuery.getResultList();
    }

    @Override
    public void deleteByNames(String sourceName, String databaseName, String tableName, List<String> partitionNames) {
        Query query = em.get().createNamedQuery(Partition.NAME_QUERY_DELETE_BY_PARTITION_NAMES);
        query.setParameter("sourceName", sourceName);
        query.setParameter("databaseName", databaseName);
        query.setParameter("tableName", tableName);
        query.setParameter("partitionNames", partitionNames);
        query.executeUpdate();
    }

    @Override
    public Integer count(String sourceName, String databaseName, String tableName) {
        TypedQuery<Integer> query = em.get().createNamedQuery(Partition.NAME_QUERY_GET_COUNT_FOR_TABLE, Integer.class);
        query.setParameter("sourceName", sourceName);
        query.setParameter("databaseName", databaseName);
        query.setParameter("tableName", tableName);
        return query.getSingleResult();
    }

    @Override
    public List<Partition> getByUri(String uri, boolean prefixSearch) {
        TypedQuery<Partition> query = null;
        if( prefixSearch){
            query = em.get().createNamedQuery(Partition.NAME_QUERY_GET_BY_URI_PREFIX, Partition.class);
            query.setParameter("uri", uri + "%");
        } else {
            query = em.get().createNamedQuery(Partition.NAME_QUERY_GET_BY_URI, Partition.class);
            query.setParameter("uri", uri);
        }
        return query.getResultList();
    }
}
