package com.netflix.metacat.usermetadata.mysql

import com.google.common.collect.Lists
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.util.DataProvider

/**
 * Created by amajumdar on 9/11/15.
 */
class MysqlUserMetadataServiceSpec extends BaseSpec{
    def testAll(){
        given:
        def catalogName = 'prodhive'
        def databaseName = 'amajumdar'
        def tableName = 'part'
        def partitionName = 'one=xyz'
        def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)
        def userName = "amajumdar"
        def table = DataProvider.getTable(catalogName, databaseName, tableName, userName, 's3:/a/b')
        def tables = DataProvider.getTables(catalogName, databaseName, tableName, userName, 's3:/a/b', 5)
        def partitions = DataProvider.getPartitions(catalogName, databaseName, tableName, partitionName, 's3:/a/b', 5)
        def uris = ['s3:/a/b0', 's3:/a/b1', 's3:/a/b2', 's3:/a/b3', 's3:/a/b4'] as List<String>
        def names = [QualifiedName.ofTable(catalogName, databaseName, tableName + '0')
                     ,QualifiedName.ofTable(catalogName, databaseName, tableName + '1')
                     ,QualifiedName.ofTable(catalogName, databaseName, tableName + '2')
                     ,QualifiedName.ofTable(catalogName, databaseName, tableName + '3')
                     ,QualifiedName.ofTable(catalogName, databaseName, tableName + '4')] as List<QualifiedName>
        when:
        mysqlUserMetadataService.saveMetadata(userName, table, true)
        then:
        mysqlUserMetadataService.getDefinitionMetadata(qualifiedName) != null
        when:
        mysqlUserMetadataService.deleteMetadatas(Lists.newArrayList(table), true)
        then:
        !mysqlUserMetadataService.getDefinitionMetadata(qualifiedName).isPresent()
        when:
        mysqlUserMetadataService.saveMetadatas( userName, partitions, true)
        then:
        mysqlUserMetadataService.getDataMetadata('s3:/a/b0') != null
        mysqlUserMetadataService.getDataMetadataMap(uris).size() == 5
        when:
        mysqlUserMetadataService.deleteDataMetadatas(uris)
        then:
        mysqlUserMetadataService.getDataMetadataMap(uris).size() == 0
        when:
        mysqlUserMetadataService.saveMetadatas( userName, tables, true)
        then:
        mysqlUserMetadataService.getDefinitionMetadataMap(names).size() == 5
        when:
        mysqlUserMetadataService.deleteMetadatas(tables, true)
        then:
        mysqlUserMetadataService.getDefinitionMetadataMap(names).size() == 0
    }
}
