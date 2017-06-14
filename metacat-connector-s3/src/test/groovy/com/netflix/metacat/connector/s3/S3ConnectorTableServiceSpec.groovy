package com.netflix.metacat.connector.s3

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.model.AuditInfo
import com.netflix.metacat.common.server.connectors.model.FieldInfo
import com.netflix.metacat.common.server.connectors.model.StorageInfo
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.connectors.exception.DatabaseNotFoundException
import com.netflix.metacat.common.type.BaseType
import com.netflix.metacat.common.type.TypeRegistry
import com.netflix.metacat.connector.pig.converters.PigTypeConverter
import com.netflix.metacat.connector.s3.dao.DatabaseDao
import com.netflix.metacat.connector.s3.dao.FieldDao
import com.netflix.metacat.connector.s3.dao.TableDao
import spock.lang.Specification

/**
 * S3 Connector database service tests.
 */
class S3ConnectorTableServiceSpec extends Specification {
    TableDao tableDao = Mock(TableDao);
    FieldDao fieldDao = Mock(FieldDao);
    DatabaseDao databaseDao = Mock(DatabaseDao);
    S3ConnectorInfoConverter converter =
        new S3ConnectorInfoConverter(new PigTypeConverter(), true, TypeRegistry.getTypeRegistry());
    S3ConnectorTableService service =
        new S3ConnectorTableService('s3', databaseDao, tableDao, fieldDao, converter)
    ConnectorRequestContext context = new ConnectorRequestContext(0, 'test')
    QualifiedName name = QualifiedName.ofTable('s3', 'd3', 't3')
    TableInfo info = TableInfo.builder().name(name)
            .fields([FieldInfo.builder().name('f1').type(BaseType.STRING).build(), FieldInfo.builder().name('f2').type(BaseType.DATE).build()])
            .auditInfo(AuditInfo.builder().createdBy('test').build())
            .serde(StorageInfo.builder().owner('test').build())
            .build()

    def testCreate(){
        when:
        service.create(context, info)
        then:
        thrown(DatabaseNotFoundException)
    }
}
