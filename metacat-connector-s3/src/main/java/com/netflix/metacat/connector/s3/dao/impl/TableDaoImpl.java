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
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.Pageable;
import com.netflix.metacat.common.dto.Sort;
import com.netflix.metacat.connector.s3.dao.TableDao;
import com.netflix.metacat.connector.s3.model.Table;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.persistence.TypedQuery;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Table DAO impl.
 */
public class TableDaoImpl extends IdEntityDaoImpl<Table> implements TableDao {
    private static final String SQL_SEARCH_TABLES =
        "select t from Table t where t.database.source.name=:sourceName and t.database.name=:databaseName"
            + " and (1=:isTableNameNull or t.name like :tableName)";
    private static final String SQL_GET_TABLE_NAMES_BY_URIS =
        "select d.name dname,t.name,uri from source s join database_object d on s.id=d.source_id join table_object t"
            + " on d.id=t.database_id join location l on t.id=l.table_id where s.name=:sourceName";
    /**
     * Constructor.
     * @param em entity manager
     */
    @Inject
    public TableDaoImpl(final Provider<EntityManager> em) {
        super(em);
    }

    @Override
    protected Class<Table> getEntityClass() {
        return Table.class;
    }

    @Override
    public Table getBySourceDatabaseTableName(final String sourceName, final String databaseName,
        final String tableName) {
        Table result = null;
        final List<Table> tables = getBySourceDatabaseTableNames(sourceName, databaseName,
            Lists.newArrayList(tableName));
        if (!tables.isEmpty()) {
            result = tables.get(0);
        }
        return result;
    }

    @Override
    public List<Table> getBySourceDatabaseTableNames(final String sourceName, final String databaseName,
        final List<String> tableNames) {
        final TypedQuery<Table> query = em.get().createNamedQuery(Table.NAME_QUERY_GET_BY_SOURCE_DATABASE_TABLE_NAMES,
            Table.class);
        query.setParameter("sourceName", sourceName);
        query.setParameter("databaseName", databaseName);
        query.setParameter("tableNames", tableNames);
        return query.getResultList();
    }

    @Override
    public List<Table> searchBySourceDatabaseTableName(final String sourceName, final String databaseName,
        final String tableNamePrefix, final Sort sort, final Pageable pageable) {
        final StringBuilder queryBuilder = new StringBuilder(SQL_SEARCH_TABLES);
        if (sort != null && sort.hasSort()) {
            queryBuilder.append(" order by ").append(sort.getSortBy()).append(" ").append(sort.getOrder().name());
        }
        final TypedQuery<Table> query = em.get().createQuery(queryBuilder.toString(), Table.class);
        query.setParameter("sourceName", sourceName);
        query.setParameter("databaseName", databaseName);
        query.setParameter("isTableNameNull", tableNamePrefix == null ? 1 : 0);
        query.setParameter("tableName", tableNamePrefix + "%");
        if (pageable != null && pageable.isPageable()) {
            query.setFirstResult(pageable.getOffset());
            query.setMaxResults(pageable.getLimit());
        }
        return query.getResultList();
    }

    @Override
    public Map<String, List<QualifiedName>> getByUris(final String sourceName, final List<String> uris,
        final boolean prefixSearch) {
        final StringBuilder builder = new StringBuilder(SQL_GET_TABLE_NAMES_BY_URIS);
        if (prefixSearch) {
            builder.append(" and ( 1=0");
            uris.forEach(uri -> builder.append(" or uri like '").append(uri).append("%'"));
            builder.append(")");
        } else {
            builder.append(" and uri in (:uris)");
        }
        final Query query = em.get().createNativeQuery(builder.toString());
        query.setParameter("sourceName", sourceName);
        if (!prefixSearch) {
            query.setParameter("uris", uris);
        }
        final List<Object[]> result = query.getResultList();
        return result.stream().collect(Collectors.groupingBy(o -> (String) o[2], Collectors
            .mapping(o -> QualifiedName.ofTable(sourceName, (String) o[0], (String) o[1]), Collectors.toList())));
    }
}
