package com.netflix.metacat.connector.s3

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.type.TypeRegistry
import com.netflix.metacat.connector.pig.converters.PigTypeConverter

/**
 * S3 Connector info converter tests.
 */
class S3ConnectorInfoConverterSpec extends BaseSpec{
    def converter = new S3ConnectorInfoConverter(new PigTypeConverter(), true, TypeRegistry.getTypeRegistry())

    def testDatabaseInfo(){
        when:
        def name = QualifiedName.ofDatabase(database.source.name, database.name)
        def info = converter.toDatabaseInfo(name, database)
        def result = converter.toDatabaseInfo(name, converter.fromDatabaseInfo(info))
        then:
        info.name.catalogName == database.source.name
        result == info
        where:
        database << databases.values()
    }

    def testTableInfo(){
        when:
        def name = QualifiedName.ofTable('s3', table.database.name, table.name)
        def info = converter.toTableInfo(name, table)
        def result = converter.toTableInfo(name, converter.fromTableInfo(info))
        then:
        info.name.databaseName == table.database.name
        result == info
        where:
        table << tables.values()
    }

    def testPartitionInfo(){
        when:
        def name = QualifiedName.ofTable('s3', table.database.name, table.name)
        def info = converter.toTableInfo(name, table)
        def result = converter.toTableInfo(name, converter.fromTableInfo(info))
        then:
        info.name.databaseName == table.database.name
        result == info
        where:
        table << tables.values()
    }
}
