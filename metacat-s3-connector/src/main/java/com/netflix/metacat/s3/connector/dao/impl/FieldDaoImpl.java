package com.netflix.metacat.s3.connector.dao.impl;

import com.netflix.metacat.s3.connector.dao.FieldDao;
import com.netflix.metacat.s3.connector.model.Field;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.persistence.EntityManager;

/**
 * Created by amajumdar on 1/2/15.
 */
public class FieldDaoImpl extends IdEntityDaoImpl<Field> implements FieldDao {
    @Inject
    public FieldDaoImpl(Provider<EntityManager> em) {
        super(em);
    }

    @Override
    protected Class<Field> getEntityClass() {
        return Field.class;
    }
}