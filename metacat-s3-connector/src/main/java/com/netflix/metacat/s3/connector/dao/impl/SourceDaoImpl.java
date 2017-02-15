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

import com.facebook.presto.exception.CatalogNotFoundException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.metacat.connector.s3.dao.SourceDao;
import com.netflix.metacat.connector.s3.model.Source;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.persistence.EntityManager;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Source DAO impl.
 */
public class SourceDaoImpl extends IdEntityDaoImpl<Source> implements SourceDao {
    private LoadingCache<String, Source> sourceCache = CacheBuilder.newBuilder().expireAfterWrite(120, TimeUnit.MINUTES)
        .build(
            new CacheLoader<String, Source>() {
                @Override
                public Source load(final String name) throws Exception {
                    return loadSource(name);
                }
            });
    /**
     * Constructor.
     * @param em entity manager
     */
    @Inject
    public SourceDaoImpl(final Provider<EntityManager> em) {
        super(em);
    }

    @Override
    protected Class<Source> getEntityClass() {
        return Source.class;
    }

    private Source loadSource(final String name) {
        return super.getByName(name);
    }

    @Override
    public Source getByName(final String name) {
        Source result = null;
        try {
            result = sourceCache.get(name);
        } catch (ExecutionException ignored) {
            //
        }
        if (result == null) {
            throw new CatalogNotFoundException(name);
        }
        return result;
    }

    @Override
    public Source getByName(final String name, final boolean fromCache) {
        if (!fromCache) {
            sourceCache.invalidate(name);
        }
        return getByName(name);
    }
}
