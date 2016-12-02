/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
 * Partition DAO impl.
 */
public class PartitionDaoImpl extends IdEntityDaoImpl<Partition> implements PartitionDao {
    private static final String SQL_GET_PARTITIONS = "select * from partition_table as p where p.table_id=:tableId";

    /**
     * Constructor.
     * @param em entity manager
     */
    @Inject
    public PartitionDaoImpl(final Provider<EntityManager> em) {
        super(em);
    }

    @Override
    protected Class<Partition> getEntityClass() {
        return Partition.class;
    }

    /**
     * Gets the partitions.
     * @param tableId table id
     * @param partitionIds partition names
     * @param partitionParts parts
     * @param dateCreatedSqlCriteria criteria
     * @param sort sort
     * @param pageable pageable
     * @return list of partitions
     */
    public List<Partition> getPartitions(final Long tableId, final List<String> partitionIds,
        final Iterable<String> partitionParts, final String dateCreatedSqlCriteria,
        final Sort sort, final Pageable pageable) {
        // Create the sql
        final StringBuilder queryBuilder = new StringBuilder(SQL_GET_PARTITIONS);
        if (partitionIds != null && !partitionIds.isEmpty()) {
            queryBuilder.append(" and p.name in ('")
                .append(Joiner.on("','").skipNulls().join(partitionIds))
                .append("')");
        }
        if (partitionParts != null) {
            for (String singlePartitionExpr : partitionParts) {
                queryBuilder.append(" and p.name like '%").append(singlePartitionExpr).append("%'");
            }
        }
        if (!Strings.isNullOrEmpty(dateCreatedSqlCriteria)) {
            queryBuilder.append(" and ").append(dateCreatedSqlCriteria);
        }
        if (sort != null && sort.hasSort()) {
            queryBuilder.append(" order by ").append(sort.getSortBy()).append(" ").append(sort.getOrder().name());
        }
        if (pageable != null && pageable.isPageable()) {
            queryBuilder.append(" limit ").append(pageable.getOffset()).append(',').append(pageable.getLimit());
        }
        // entityManager
        final EntityManager entityManager = em.get();
        final Query pQuery = entityManager.createNativeQuery(queryBuilder.toString(), Partition.class);
        pQuery.setParameter("tableId", tableId);
        return pQuery.getResultList();
    }

    @Override
    public void deleteByNames(final String sourceName, final String databaseName, final String tableName,
        final List<String> partitionNames) {
        final Query query = em.get().createNamedQuery(Partition.NAME_QUERY_DELETE_BY_PARTITION_NAMES);
        query.setParameter("sourceName", sourceName);
        query.setParameter("databaseName", databaseName);
        query.setParameter("tableName", tableName);
        query.setParameter("partitionNames", partitionNames);
        query.executeUpdate();
    }

    @Override
    public Integer count(final String sourceName, final String databaseName, final String tableName) {
        final TypedQuery<Integer> query = em.get().createNamedQuery(Partition.NAME_QUERY_GET_COUNT_FOR_TABLE,
            Integer.class);
        query.setParameter("sourceName", sourceName);
        query.setParameter("databaseName", databaseName);
        query.setParameter("tableName", tableName);
        return query.getSingleResult();
    }

    @Override
    public List<Partition> getByUris(final List<String> uris, final boolean prefixSearch) {
        TypedQuery<Partition> query = null;
        if (prefixSearch) {
            final StringBuilder builder = new StringBuilder("select p from Partition p where 1=2");
            uris.forEach(uri -> builder.append(" or uri like '").append(uri).append("%'"));
            query = em.get().createNamedQuery(builder.toString(), Partition.class);
        } else {
            query = em.get().createNamedQuery(Partition.NAME_QUERY_GET_BY_URI, Partition.class);
            query.setParameter("uris", uris);
        }
        return query.getResultList();
    }
}
