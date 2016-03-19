package com.netflix.metacat.s3.connector.dao.impl;

import com.google.common.collect.Lists;
import com.netflix.metacat.s3.connector.dao.DatabaseDao;
import com.netflix.metacat.s3.connector.model.Database;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import java.util.List;

/**
 * Created by amajumdar on 1/2/15.
 */
public class DatabaseDaoImpl extends IdEntityDaoImpl<Database> implements DatabaseDao {
    @Inject
    public DatabaseDaoImpl(Provider<EntityManager> em) {
        super(em);
    }

    @Override
    protected Class<Database> getEntityClass() {
        return Database.class;
    }

    @Override
    public Database getBySourceDatabaseName(String sourceName, String databaseName) {
        Database result = null;
        List<Database> databases = getBySourceDatabaseNames(sourceName, Lists.newArrayList(databaseName));
        if( !databases.isEmpty()){
            result = databases.get(0);
        }
        return result;
    }

    @Override
    public List<Database> getBySourceDatabaseNames(String sourceName, List<String> databaseNames) {
        TypedQuery<Database> query = em.get().createNamedQuery(Database.NAME_QUERY_GET_BY_SOURCE_DATABASE_NAMES,
                Database.class);
        query.setParameter("sourceName", sourceName);
        query.setParameter("databaseNames", databaseNames);
        return query.getResultList();
    }
}
