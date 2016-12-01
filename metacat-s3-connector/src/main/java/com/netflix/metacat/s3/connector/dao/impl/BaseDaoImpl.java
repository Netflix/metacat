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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.metacat.s3.connector.dao.BaseDao;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.inject.Provider;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import java.util.List;

/**
 * Base DAO implementation.
 * @param <T>
 */
public abstract class BaseDaoImpl<T> implements BaseDao<T> {
    private static final String SQL_GET_BY_NAME = "select a from %s a where name=:name";
    private static final String SQL_GET_BY_NAMES = "select a from %s a where name in (:names)";
    protected Provider<EntityManager> em;

    protected BaseDaoImpl(final Provider<EntityManager> em) {
        this.em = em;
    }

    protected abstract Class<T> getEntityClass();

    @Override
    public T save(final T entity) {
        return save(entity, false);
    }

    protected abstract boolean isNew(T entity);

    @Override
    public T save(final T entity, final boolean flush) {
        T result = null;
        final EntityManager entityManager = em.get();
        if (isNew(entity)) {
            entityManager.persist(entity);
            result = entity;
        } else {
            result = entityManager.merge(entity);
        }
        if (flush) {
            entityManager.flush();
        }

        return result;
    }

    @Override
    public List<T> save(final Iterable<T> entities) {
        final List<T> result = Lists.newArrayList();

        if (entities != null) {
            for (T entity : entities) {
                result.add(save(entity));
            }
        }

        return result;
    }

    @Override
    public void deleteById(final Long id) {
        Preconditions.checkArgument(id != null, "Id cannot be null.");
        final T entity = get(id);
        if (entity != null) {
            delete(entity);
        }
    }

    @Override
    public void deleteById(final Iterable<Long> ids) {
        Preconditions.checkArgument(ids != null, "Ids cannot be null.");
        for (Long id : ids) {
            deleteById(id);
        }
    }

    @Override
    public void delete(final T entity) {
        Preconditions.checkArgument(entity != null, "Entity cannot be null.");
        final EntityManager entityManager = em.get();
        entityManager.remove(entity);
    }

    @Override
    public void delete(final Iterable<T> entities) {
        Preconditions.checkArgument(entities != null, "Entities cannot be null.");
        for (T entity : entities) {
            delete(entity);
        }
    }

    @Override
    public void deleteAll() {
        em.get().createQuery("delete from " + getEntityClass().getName()).executeUpdate();
    }

    @Override
    public boolean isExists(final Long id) {
        return get(id) != null;
    }

    @Override
    public T get(final Long id) {
        Preconditions.checkArgument(id != null, "Id cannot be null.");
        return em.get().find(getEntityClass(), id);
    }

    @Override
    @SuppressFBWarnings
    public T getByName(final String name) {
        T result = null;
        Preconditions.checkArgument(name != null, "Name cannot be null.");
        final TypedQuery<T> query = em.get()
            .createQuery(String.format(SQL_GET_BY_NAME, getEntityClass().getName()), getEntityClass());
        query.setParameter("name", name);
        try {
            result = query.getSingleResult();
        } catch (Exception ignored) { }
        return result;
    }

    @Override
    public List<T> getByNames(final List<String> names) {
        List<T> result = Lists.newArrayList();
        if (names != null && !names.isEmpty()) {
            final TypedQuery<T> query = em.get()
                .createQuery(String.format(SQL_GET_BY_NAMES, getEntityClass().getName()), getEntityClass());
            query.setParameter("names", names);
            result = query.getResultList();
        }
        return result;
    }

    @Override
    public List<T> get(final Iterable<Long> ids) {
        final List<T> result = Lists.newArrayList();
        for (Long id : ids) {
            result.add(get(id));
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<T> getAll() {
        return em.get().createQuery("select a from " + getEntityClass().getName() + " a").getResultList();
    }

    @Override
    public long count() {
        return (long) em.get().createQuery("select count(a) from " + getEntityClass().getName() + " a")
            .getSingleResult();
    }
}
