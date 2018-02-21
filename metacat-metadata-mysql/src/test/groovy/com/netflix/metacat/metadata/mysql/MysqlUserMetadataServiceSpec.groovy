/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.metadata.mysql

import com.google.common.collect.Lists
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.testdata.provider.DataDtoProvider

/**
 * Tests for MysqlUserMetadataService.
 * TODO: Need to move this to integration-test
 * @author amajumdar
 */
class MysqlUserMetadataServiceSpec extends BaseSpec {
    def testAll() {
        given:
        def catalogName = 'prodhive'
        def databaseName = 'amajumdar'
        def tableName = 'part'
        def partitionName = 'one=xyz'
        def qualifiedName = QualifiedName.ofTable(catalogName, databaseName, tableName)
        def userName = "amajumdar"
        def table = DataDtoProvider.getTable(catalogName, databaseName, tableName, userName, 's3:/a/b')
        def table2 = DataDtoProvider.getTable(catalogName, databaseName, tableName, "overrideUser", 's3:/a/b')
        def tables = DataDtoProvider.getTables(catalogName, databaseName, tableName, userName, 's3:/a/b', 5)
        def partitions = DataDtoProvider.getPartitions(catalogName, databaseName, tableName, partitionName, 's3:/a/b', 5)
        def uris = ['s3:/a/b0', 's3:/a/b1', 's3:/a/b2', 's3:/a/b3', 's3:/a/b4'] as List<String>
        def names = [QualifiedName.ofTable(catalogName, databaseName, tableName + '0')
                     , QualifiedName.ofTable(catalogName, databaseName, tableName + '1')
                     , QualifiedName.ofTable(catalogName, databaseName, tableName + '2')
                     , QualifiedName.ofTable(catalogName, databaseName, tableName + '3')
                     , QualifiedName.ofTable(catalogName, databaseName, tableName + '4')] as List<QualifiedName>
        when:
        mysqlUserMetadataService.saveMetadata(userName, table2, true)
        then:
        mysqlUserMetadataService.getDefinitionMetadata(qualifiedName).isPresent()
        when:
        mysqlUserMetadataService.saveMetadata(userName, table, false)
        then:
        mysqlUserMetadataService.getDefinitionMetadata(qualifiedName).isPresent()
        mysqlUserMetadataService.getDefinitionMetadata(qualifiedName).get().findValue('userId').toString().equals("\"amajumdar\"")

        when:
        mysqlUserMetadataService.deleteMetadatas("test", Lists.newArrayList(table))
        then:
        !mysqlUserMetadataService.getDefinitionMetadata(qualifiedName).isPresent()
        mysqlUserMetadataService.getDescendantDefinitionNames(qualifiedName).size() == 0
        when:
        mysqlUserMetadataService.saveMetadatas(userName, partitions, true)
        then:
        mysqlUserMetadataService.getDataMetadata('s3:/a/b0').isPresent()
        mysqlUserMetadataService.getDataMetadataMap(uris).size() == 5
        mysqlUserMetadataService.getDescendantDataUris('s3:/a').size() == 5
        mysqlUserMetadataService.getDescendantDataUris('s3:/z').size() == 0
        when:
        mysqlUserMetadataService.deleteDataMetadatas(uris)
        then:
        mysqlUserMetadataService.getDataMetadataMap(uris).size() == 0
        when:
        mysqlUserMetadataService.saveMetadatas(userName, tables, true)
        then:
        mysqlUserMetadataService.getDefinitionMetadataMap(names).size() == 5
        mysqlUserMetadataService.getDescendantDefinitionNames(QualifiedName.ofDatabase(catalogName, databaseName)).size() == 5
        when:
        mysqlUserMetadataService.deleteMetadatas("test", tables)
        then:
        mysqlUserMetadataService.getDefinitionMetadataMap(names).size() == 0
        mysqlUserMetadataService.getDescendantDataUris('s3:/a').size() == 0
        sleep(1000)
        mysqlUserMetadataService.getDeletedDataMetadataUris(new Date(), 0, 10).size() == 0
        mysqlUserMetadataService.getDeletedDataMetadataUris(new Date(1, 1, 1), 0, 10).size() == 0
        when:
        mysqlUserMetadataService.deleteDataMetadatas(uris)
        then:
        mysqlUserMetadataService.getDeletedDataMetadataUris(new Date(), 0, 10).size() == 0
        when:
        mysqlUserMetadataService.saveMetadatas(userName, partitions, true)
        mysqlUserMetadataService.deleteMetadatas("test", partitions)
        mysqlUserMetadataService.deleteMetadatas("test", partitions)
        then:
        mysqlUserMetadataService.getDefinitionMetadataMap(names).size() == 0
        mysqlUserMetadataService.getDescendantDataUris('s3:/a').size() == 5
        sleep(1000)
        mysqlUserMetadataService.getDeletedDataMetadataUris(new Date(), 0, 10).size() == 5
        mysqlUserMetadataService.getDeletedDataMetadataUris(new Date(1, 1, 1), 0, 10).size() == 0
        when:
        mysqlUserMetadataService.deleteDataMetadatas(uris)
        then:
        mysqlUserMetadataService.getDeletedDataMetadataUris(new Date(), 0, 10).size() == 0
    }
}
