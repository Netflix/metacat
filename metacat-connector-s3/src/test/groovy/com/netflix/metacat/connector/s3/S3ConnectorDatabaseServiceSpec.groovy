package com.netflix.metacat.connector.s3

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import com.netflix.metacat.common.server.connectors.exception.ConnectorException
import com.netflix.metacat.common.server.connectors.exception.DatabaseAlreadyExistsException
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException
import com.netflix.metacat.common.type.TypeRegistry
import com.netflix.metacat.connector.pig.converters.PigTypeConverter
import com.netflix.metacat.connector.s3.dao.DatabaseDao
import com.netflix.metacat.connector.s3.dao.SourceDao
import com.netflix.metacat.connector.s3.model.Database
import com.netflix.metacat.connector.s3.model.Table
import spock.lang.Specification

/**
 * S3 Connector database service tests.
 */
class S3ConnectorDatabaseServiceSpec extends Specification {
    SourceDao sourceDao = Mock(SourceDao);
    DatabaseDao databaseDao = Mock(DatabaseDao);
    S3ConnectorInfoConverter converter =
        new S3ConnectorInfoConverter(new PigTypeConverter(), true, TypeRegistry.getTypeRegistry());
    S3ConnectorDatabaseService service =
        new S3ConnectorDatabaseService('s3', databaseDao, sourceDao, converter)
    ConnectorRequestContext context = new ConnectorRequestContext(timestamp:0, userName:'test')
    QualifiedName databaseName = QualifiedName.ofDatabase('s3', 'd3')

    def testListViewNames(){
        when:
        def names = service.listViewNames(context, databaseName)
        then:
        noExceptionThrown()
        names.size() == 0
    }

    def testCreate(){
        when:
        service.create(context, DatabaseInfo.builder().name(databaseName).build())
        then:
        noExceptionThrown()
        1 * databaseDao.getBySourceDatabaseName(databaseName.catalogName, databaseName.databaseName)
        1 * sourceDao.getByName(databaseName.catalogName)
        when:
        service.create(context, DatabaseInfo.builder().name(databaseName).build())
        then:
        thrown(DatabaseAlreadyExistsException)
        1 * databaseDao.getBySourceDatabaseName(databaseName.catalogName, databaseName.databaseName) >> new Database()
        0 * sourceDao.getByName(databaseName.catalogName)
    }

    def testDelete(){
        when:
        service.delete(context, databaseName)
        then:
        thrown(DatabaseNotFoundException)
        when:
        service.delete(context, databaseName)
        then:
        noExceptionThrown()
        1 * databaseDao.getBySourceDatabaseName(databaseName.catalogName, databaseName.databaseName) >> new Database()
        when:
        service.create(context, DatabaseInfo.builder().name(databaseName).build())
        then:
        thrown(ConnectorException)
        1 * databaseDao.getBySourceDatabaseName(databaseName.catalogName, databaseName.databaseName) >> new Database(tables: [new Table()])
    }

    def testGet(){
        when:
        def db = new Database(name: 'd3')
        def info = service.get(context, databaseName)
        then:
        thrown(DatabaseNotFoundException)
        when:
        info = service.get(context, databaseName)
        then:
        noExceptionThrown()
        1 * databaseDao.getBySourceDatabaseName(databaseName.catalogName, databaseName.databaseName) >> db
        info == converter.toDatabaseInfo(QualifiedName.ofCatalog('s3'), db)
    }

    def testRename(){
        when:
        def db = new Database(name: 'd3')
        def newName = QualifiedName.ofDatabase('s3', 'd4')
        service.rename(context, databaseName, newName)
        then:
        thrown(DatabaseNotFoundException)
        when:
        service.rename(context, databaseName, newName)
        then:
        thrown(DatabaseAlreadyExistsException)
        1 * databaseDao.getBySourceDatabaseName(databaseName.catalogName, databaseName.databaseName) >> db
        1 * databaseDao.getBySourceDatabaseName(newName.catalogName, newName.databaseName) >> db
        when:
        service.rename(context, databaseName, newName)
        then:
        noExceptionThrown()
        1 * databaseDao.getBySourceDatabaseName(databaseName.catalogName, databaseName.databaseName) >> db
    }
}
