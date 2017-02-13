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

package com.netflix.metacat.connector.s3.dao.impl;

import com.google.common.collect.Lists;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.connector.s3.dao.DatabaseDao;
import com.netflix.metacat.connector.s3.model.Database;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import java.util.List;

/**
 * Database DAO implementation.
 */
public class DatabaseDaoImpl extends IdEntityDaoImpl<Database> implements DatabaseDao {
    private static final String SQL_SEARCH_DATABASES =
        "select d from Database d where d.source.name=:sourceName and (1=:isNameNull or d.name like :databaseName)";
    /**
     * Constructor.
     * @param em entity manager
     */
    @Inject
    public DatabaseDaoImpl(final Provider<EntityManager> em) {
        super(em);
    }

    @Override
    protected Class<Database> getEntityClass() {
        return Database.class;
    }

    @Override
    public Database getBySourceDatabaseName(final String sourceName, final String databaseName) {
        Database result = null;
        final List<Database> databases = getBySourceDatabaseNames(sourceName, Lists.newArrayList(databaseName));
        if (!databases.isEmpty()) {
            result = databases.get(0);
        }
        return result;
    }

    @Override
    public List<Database> getBySourceDatabaseNames(final String sourceName, final List<String> databaseNames) {
        final TypedQuery<Database> query = em.get().createNamedQuery(Database.NAME_QUERY_GET_BY_SOURCE_DATABASE_NAMES,
            Database.class);
        query.setParameter("sourceName", sourceName);
        query.setParameter("databaseNames", databaseNames);
        return query.getResultList();
    }

    @Override
    public List<Database> searchBySourceDatabaseName(final String sourceName, final String databaseNamePrefix,
        final Sort sort, final Pageable pageable) {
        final StringBuilder queryBuilder = new StringBuilder(SQL_SEARCH_DATABASES);
        if (sort != null && sort.hasSort()) {
            queryBuilder.append(" order by ").append(sort.getSortBy()).append(" ").append(sort.getOrder().name());
        }
        // entityManager
        final EntityManager entityManager = em.get();
        final TypedQuery<Database> pQuery = entityManager.createQuery(queryBuilder.toString(), Database.class);
        pQuery.setParameter("sourceName", sourceName);
        pQuery.setParameter("isNameNull", databaseNamePrefix == null ? 1 : 0);
        pQuery.setParameter("databaseName", databaseNamePrefix + "%");
        if (pageable != null && pageable.isPageable()) {
            pQuery.setFirstResult(pageable.getOffset());
            pQuery.setMaxResults(pageable.getLimit());
        }
        return pQuery.getResultList();
    }
}
