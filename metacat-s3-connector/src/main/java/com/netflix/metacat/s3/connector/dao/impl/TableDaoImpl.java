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
import com.netflix.metacat.connector.s3.dao.TableDao;
import com.netflix.metacat.connector.s3.model.Table;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import java.util.List;

/**
 * Table DAO impl.
 */
public class TableDaoImpl extends IdEntityDaoImpl<Table> implements TableDao {
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
}
