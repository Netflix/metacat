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

package com.netflix.metacat.s3.connector.dao.impl

import com.netflix.metacat.s3.connector.BaseSpec
import com.netflix.metacat.s3.connector.dao.DatabaseDao
import com.netflix.metacat.s3.connector.dao.SourceDao
import com.netflix.metacat.s3.connector.model.Database
import com.netflix.metacat.s3.connector.model.Source

import javax.inject.Inject
import javax.persistence.EntityManager

/**
 * Created by amajumdar on 10/12/15.
 */
class DatabaseDaoImplSpec extends BaseSpec{
    @Inject
    DatabaseDao databaseDao
    @Inject
    SourceDao sourceDao
    @Inject
    EntityManager em
    def testAll(){
        given:
        def source = new Source(name:'s3', type:'s3')
        def database = new Database(name: 'test', source: source)
        def tx = em.getTransaction()
        when:
        tx.begin()
        sourceDao.save(source, true)
        databaseDao.save( database, true)
        tx.commit()
        then:
        databaseDao.getBySourceDatabaseName('s3', 'test') != null
        when:
        tx.begin()
        databaseDao.deleteById(database.id)
        tx.commit()
        then:
        databaseDao.get(database.id) == null
    }

    def testCascade(){
        given:
        def source = new Source(name:'c3', type:'c3')
        def database = new Database(name: 'cascade', source: source)
        source.setDatabases([database])
        def tx = em.getTransaction()
        when:
        tx.begin()
        sourceDao.save(source, true)
        tx.commit()
        then:
        databaseDao.getBySourceDatabaseName('c3', 'cascade') != null
        when:
        tx.begin()
        sourceDao.delete([source])
        tx.commit()
        then:
        databaseDao.get([database.id]).size() == 0
    }
}
