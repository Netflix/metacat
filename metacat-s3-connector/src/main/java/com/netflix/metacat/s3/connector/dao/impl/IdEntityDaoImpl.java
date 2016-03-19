package com.netflix.metacat.s3.connector.dao.impl;

import com.google.common.collect.Lists;
import com.netflix.metacat.s3.connector.dao.BaseDao;
import com.netflix.metacat.s3.connector.model.IdEntity;

import javax.inject.Provider;
import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.List;

/**
 * Created by amajumdar on 1/2/15.
 */
public abstract class IdEntityDaoImpl<T extends IdEntity> extends BaseDaoImpl<T> implements
        BaseDao<T> {

    protected IdEntityDaoImpl(Provider<EntityManager> em) {
        super(em);
    }

    @Override
    public List<T> get(Iterable<Long> ids) {
        EntityManager entityManager = em.get();
        CriteriaBuilder cb = entityManager.getCriteriaBuilder();
        CriteriaQuery<T> criteriaQuery = cb.createQuery(getEntityClass());
        Root<T> root = criteriaQuery.from(getEntityClass());
        criteriaQuery.where(root.get("id").in(Lists.newArrayList(ids)));
        return entityManager.createQuery(criteriaQuery).getResultList();
    }

    @Override
    protected boolean isNew(T entity) {
        return entity.getId()==null;
    }
}