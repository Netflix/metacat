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

package com.netflix.metacat.connector.s3.dao.impl

import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.connector.s3.BaseSpec
import com.netflix.metacat.connector.s3.dao.DatabaseDao
import com.netflix.metacat.connector.s3.dao.PartitionDao
import com.netflix.metacat.connector.s3.dao.SourceDao
import com.netflix.metacat.connector.s3.dao.TableDao
import com.netflix.metacat.connector.s3.model.Database
import com.netflix.metacat.connector.s3.model.Source

import javax.inject.Inject
import javax.persistence.EntityManager

/**
 * Created by amajumdar on 10/12/15.
 */
class DaoImplSpec extends BaseSpec{
    @Inject DatabaseDao databaseDao
    @Inject SourceDao sourceDao
    @Inject TableDao tableDao
    @Inject PartitionDao partitionDao
    @Inject EntityManager em
    def testDatabase(){
        given:
        def source = sources.get('s3')
        def database = databases.get('test')
        def database1 = databases.get('test1')
        def tx = em.getTransaction()
        when:
        tx.begin()
        sourceDao.save(source, true)
        databaseDao.save( database, true)
        databaseDao.save([database, database1])
        tx.commit()
        then:
        databaseDao.getBySourceDatabaseName('s3', 'test') != null
        databaseDao.getBySourceDatabaseName('s3', 'test') == database
        databaseDao.getBySourceDatabaseNames('s3', ['test']) == [database]
        databaseDao.searchBySourceDatabaseName('s3', 't', null, null).size() == 2
        databaseDao.searchBySourceDatabaseName('s3', 'test', null, new Pageable(limit:2)).size() == 2
        databaseDao.searchBySourceDatabaseName('s3', 'test1', null, null).get(0).name == 'test1'
        when:
        tx.begin()
        databaseDao.deleteById(database.id)
        tx.commit()
        then:
        databaseDao.get(database.id) == null
    }

    def testTable(){
        given:
        def source = sources.get('s3')
        def database = databases.get('test')
        def table = tables.get('part')
        def table1 = tables.get('part1')
        def tx = em.getTransaction()
        when:
        tx.begin()
        sourceDao.save(source, true)
        def databaseEntity = databaseDao.save( database, true)
        table.setDatabase(databaseEntity)
        table1.setDatabase(databaseEntity)
        tableDao.save(table)
        tableDao.save([table, table1])
        tx.commit()
        then:
        tableDao.count() == 2
        tableDao.getBySourceDatabaseTableName('s3', 'test', 'part') == table
        tableDao.getBySourceDatabaseTableName('s3', 'test', 'part').location.schema.fields.size() == 1
        tableDao.getBySourceDatabaseTableNames('s3', 'test', ['part','part1']).size() == 2
        tableDao.searchBySourceDatabaseTableName('s3', 'test', 'part', null, null).size() == 2
        tableDao.searchBySourceDatabaseTableName('s3', 'test', 'part', null, new Pageable(limit:1)).size() == 1
        tableDao.searchBySourceDatabaseTableName('s3', 'test', 'part', null, new Pageable(limit:1))
            .get(0).location.schema.fields.size() == 1
        tableDao.searchBySourceDatabaseTableName('s3', 'test', 'part', null, new Pageable(limit:1))
            .get(0).name == 'part'
        tableDao.getByUris('s3', ['s3://'], false).size() == 1
        tableDao.getByUris('s3', ['s3://'], true).size() == 1
        tableDao.getByUris('s3', ['s3://asd'], false).size() == 0
        tableDao.getByUris('s3', [''], false).size() == 0
        when:
        tx.begin()
        tableDao.delete(table)
        tx.commit()
        then:
        tableDao.get(table.id) == null
        when:
        tx.begin()
        tableDao.delete(table1)
        databaseDao.delete(databaseEntity)
        tx.commit()
        then:
        tableDao.get([table1.id]).size() == 0
    }

    def testPartition(){
        given:
        def table = tables.get('part')
        table.setPartitions([partitions.get('dateint=20171212')])
        def database = databases.get('test')
        table.setDatabase(database)
        database.setTables([table])
        def source = sources.get('s3')
        source.setDatabases([database])
        def tx = em.getTransaction()
        when:
        tx.begin()
        sourceDao.save(source, true)
        tx.commit()
        then:
        partitionDao.count('s3', 'test', 'part') == 1L
        when:
        tx.begin()
        table = source.databases[0].tables[0]
        partitions.values().each {p -> p.setTable(table)}
        partitionDao.save([partitions.get('dateint=20171213'), partitions.get('dateint=20171214')])
        tx.commit()
        then:
        partitionDao.count('s3', 'test', 'part') == 3L
        partitionDao.getPartitions(table.id, ['dateint=20171212'], null, null, null, null).size() == 1
        partitionDao.getPartitions(table.id, ['dateint=20170230'], null, null, null, null).size() == 0
        partitionDao.getPartitions(table.id, null, null, null, null, new Pageable(limit:2)).size() == 2
        when:
        tx.begin()
        partitionDao.deleteByNames('s3', 'test', 'part', ['dateint=20171212', 'dateint=20171213', 'dateint=20171214'])
        tx.commit()
        then:
        partitionDao.count('s3', 'test', 'part') == 0L
        partitionDao.getPartitions(table.id, ['dateint=20171212'], null, null, null, null).size() == 0
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
