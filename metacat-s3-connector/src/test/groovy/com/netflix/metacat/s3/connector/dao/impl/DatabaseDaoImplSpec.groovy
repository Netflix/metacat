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
