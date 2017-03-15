/*
 *
 *  Copyright 2017 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.metacat.connector.mysql

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
/**
 * Tests for the MySqlConnectorDatabaseService.
 *
 * @author tgianos
 * @since 1.0.0
 */
class MySqlConnectorDatabaseServiceSpec extends Specification {

    def dataSource = Mock(DataSource)
    def connection = Mock(Connection)
    def context = Mock(ConnectorContext)

    def service = new MySqlConnectorDatabaseService(this.dataSource)

    def "Can get list of database names without prefix, sort by or pagination"() {
        def qName = QualifiedName.ofCatalog(UUID.randomUUID().toString())
        def dbMetadata = Mock(DatabaseMetaData)
        def schemaResultSet = Mock(ResultSet)

        when:
        def databases = this.service.listNames(this.context, qName, null, null, null)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.getMetaData() >> dbMetadata
        1 * dbMetadata.getCatalogs() >> schemaResultSet
        8 * schemaResultSet.next() >>> [true, true, true, true, true, true, true, false]
        7 * schemaResultSet.getString("TABLE_CAT") >>> ["a", "b", "c", "d", "e", "information_schema", "mysql"]
        databases.size() == 5
        databases.get(0).getDatabaseName() == "a"
        databases.get(1).getDatabaseName() == "b"
        databases.get(2).getDatabaseName() == "c"
        databases.get(3).getDatabaseName() == "d"
        databases.get(4).getDatabaseName() == "e"
    }

    def "Can get list of database names with prefix but without sort by or pagination"() {
        def qName = QualifiedName.ofCatalog(UUID.randomUUID().toString())
        def prefix = QualifiedName.ofDatabase(UUID.randomUUID().toString(), "a")
        def dbMetadata = Mock(DatabaseMetaData)
        def schemaResultSet = Mock(ResultSet)

        when:
        def databases = this.service.listNames(this.context, qName, prefix, null, null)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.getMetaData() >> dbMetadata
        1 * dbMetadata.getCatalogs() >> schemaResultSet
        5 * schemaResultSet.next() >>> [true, true, true, true, false]
        4 * schemaResultSet.getString("TABLE_CAT") >>> ["a", "ac", "ad", "bg"]
        databases.size() == 3
        databases.get(0).getDatabaseName() == "a"
        databases.get(1).getDatabaseName() == "ac"
        databases.get(2).getDatabaseName() == "ad"
    }

    def "Can get list of database names sorted in ascending order"() {
        def qName = QualifiedName.ofCatalog(UUID.randomUUID().toString())
        def dbMetadata = Mock(DatabaseMetaData)
        def schemaResultSet = Mock(ResultSet)
        def sort = new Sort()
        sort.setOrder(SortOrder.ASC)

        when:
        def databases = this.service.listNames(this.context, qName, null, sort, null)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.getMetaData() >> dbMetadata
        1 * dbMetadata.getCatalogs() >> schemaResultSet
        4 * schemaResultSet.next() >>> [true, true, true, false]
        3 * schemaResultSet.getString("TABLE_CAT") >>> ["a", "c", "b"]
        databases.size() == 3
        databases.get(0).getDatabaseName() == "a"
        databases.get(1).getDatabaseName() == "b"
        databases.get(2).getDatabaseName() == "c"
    }

    def "Can get list of database names sorted in descending order"() {
        def qName = QualifiedName.ofCatalog(UUID.randomUUID().toString())
        def dbMetadata = Mock(DatabaseMetaData)
        def schemaResultSet = Mock(ResultSet)
        def sort = new Sort()
        sort.setOrder(SortOrder.DESC)

        when:
        def databases = this.service.listNames(this.context, qName, null, sort, null)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.getMetaData() >> dbMetadata
        1 * dbMetadata.getCatalogs() >> schemaResultSet
        4 * schemaResultSet.next() >>> [true, true, true, false]
        3 * schemaResultSet.getString("TABLE_CAT") >>> ["a", "c", "b"]
        databases.size() == 3
        databases.get(0).getDatabaseName() == "c"
        databases.get(1).getDatabaseName() == "b"
        databases.get(2).getDatabaseName() == "a"
    }

    def "Can get list of database names with pagination"() {
        def qName = QualifiedName.ofCatalog(UUID.randomUUID().toString())
        def dbMetadata = Mock(DatabaseMetaData)
        def schemaResultSet = Mock(ResultSet)
        def sort = new Sort()
        sort.setOrder(SortOrder.ASC)
        def pageable = new Pageable(1, 2)

        when:
        def databases = this.service.listNames(this.context, qName, null, sort, pageable)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.getMetaData() >> dbMetadata
        1 * dbMetadata.getCatalogs() >> schemaResultSet
        4 * schemaResultSet.next() >>> [true, true, true, false]
        3 * schemaResultSet.getString("TABLE_CAT") >>> ["a", "c", "b"]
        databases.size() == 1
        databases.get(0).getDatabaseName() == "c"
    }

    def "Paging beyond number of databases returns empty list of names"() {
        def qName = QualifiedName.ofCatalog(UUID.randomUUID().toString())
        def dbMetadata = Mock(DatabaseMetaData)
        def schemaResultSet = Mock(ResultSet)
        def sort = new Sort()
        sort.setOrder(SortOrder.ASC)
        def pageable = new Pageable(1, 3)

        when:
        def databases = this.service.listNames(this.context, qName, null, sort, pageable)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.getMetaData() >> dbMetadata
        1 * dbMetadata.getCatalogs() >> schemaResultSet
        4 * schemaResultSet.next() >>> [true, true, true, false]
        3 * schemaResultSet.getString("TABLE_CAT") >>> ["a", "c", "b"]
        databases.size() == 0
    }
}
