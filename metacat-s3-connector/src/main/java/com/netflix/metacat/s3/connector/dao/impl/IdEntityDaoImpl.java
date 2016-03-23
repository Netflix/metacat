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