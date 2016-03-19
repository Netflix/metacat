package com.netflix.metacat.s3.connector.dao.impl;

import com.google.common.collect.Lists;
import com.netflix.metacat.s3.connector.dao.TableDao;
import com.netflix.metacat.s3.connector.model.Table;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import java.util.List;

/**
 * Created by amajumdar on 1/2/15.
 */
public class TableDaoImpl extends IdEntityDaoImpl<Table> implements TableDao {
    @Inject
    public TableDaoImpl(Provider<EntityManager> em) {
        super(em);
    }

    @Override
    protected Class<Table> getEntityClass() {
        return Table.class;
    }

    @Override
    public Table getBySourceDatabaseTableName(String sourceName, String databaseName, String tableName) {
        Table result = null;
        List<Table> tables = getBySourceDatabaseTableNames(sourceName, databaseName, Lists.newArrayList(tableName));
        if( !tables.isEmpty()){
            result = tables.get(0);
        }
        return result;
    }

    @Override
    public List<Table> getBySourceDatabaseTableNames(String sourceName, String databaseName, List<String> tableNames) {
        TypedQuery<Table> query = em.get().createNamedQuery(Table.NAME_QUERY_GET_BY_SOURCE_DATABASE_TABLE_NAMES,
                Table.class);
        query.setParameter("sourceName", sourceName);
        query.setParameter("databaseName", databaseName);
        query.setParameter("tableNames", tableNames);
        return query.getResultList();
    }
}