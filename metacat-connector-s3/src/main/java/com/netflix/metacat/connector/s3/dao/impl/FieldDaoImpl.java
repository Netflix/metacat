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

import com.netflix.metacat.connector.s3.dao.FieldDao;
import com.netflix.metacat.connector.s3.model.Field;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.persistence.EntityManager;

/**
 * Field DAO impl.
 */
public class FieldDaoImpl extends IdEntityDaoImpl<Field> implements FieldDao {
    /**
     * Constructor.
     * @param em entity manager
     */
    @Inject
    public FieldDaoImpl(final Provider<EntityManager> em) {
        super(em);
    }

    @Override
    protected Class<Field> getEntityClass() {
        return Field.class;
    }
}
