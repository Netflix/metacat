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
package com.netflix.metacat.connector.jdbc.services

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.*

/**
 * Tests for JdbcConnectorDatabaseService.
 *
 * @author tgianos
 * @since 0.1.52
 */
class JdbcConnectorDatabaseServiceSpec extends Specification {

    def dataSource = Mock(DataSource)
    def connection = Mock(Connection)
    def statement = Mock(Statement)
    def context = Mock(ConnectorContext)

    def service = new JdbcConnectorDatabaseService(this.dataSource)

    def "Can create a database"() {
        def databaseName = UUID.randomUUID().toString()
        def database = new DatabaseInfo.Builder(QualifiedName.ofDatabase(UUID.randomUUID().toString(), databaseName))
            .build()

        when:
        this.service.create(this.context, database)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.createStatement() >> this.statement
        1 * this.statement.executeUpdate("CREATE DATABASE " + databaseName) >> 1
    }

    def "Can't create a database on SQL exception"() {
        def databaseName = UUID.randomUUID().toString()
        def database = new DatabaseInfo.Builder(QualifiedName.ofDatabase(UUID.randomUUID().toString(), databaseName))
            .build()

        when:
        this.service.create(this.context, database)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.createStatement() >> this.statement
        1 * this.statement.executeUpdate("CREATE DATABASE " + databaseName) >> { throw new SQLException("blah") }
        thrown RuntimeException
    }

    def "Can drop a database"() {
        def databaseName = UUID.randomUUID().toString()
        def qName = QualifiedName.ofDatabase(UUID.randomUUID().toString(), databaseName)

        when:
        this.service.delete(this.context, qName)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.createStatement() >> this.statement
        1 * this.statement.executeUpdate("DROP DATABASE " + databaseName) >> 1
    }

    def "Can get a Database Info Instance"() {
//        def catalog = UUID.randomUUID().toString()
        def databaseName = UUID.randomUUID().toString()
        def qName = QualifiedName.ofDatabase(UUID.randomUUID().toString(), databaseName)
//        def dbMetadata = Mock(DatabaseMetaData)
//        def resultSet = Mock(ResultSet)
//        def expectedTables = ["one", "two", "three"]

        when:
        def db = this.service.get(this.context, qName)

        then:
        db.getName() == qName
//        1 * this.dataSource.getConnection() >> this.connection
//        1 * this.connection.getMetaData() >> dbMetadata
//        1 * this.connection.getCatalog() >> catalog
//        1 * dbMetadata.getSearchStringEscape() >> "%"
//        1 * dbMetadata.getTables(catalog, databaseName, "%", null) >> resultSet
//        4 * resultSet.next() >>> [true, true, true, false]
//        3 * resultSet.getString("TABLE_NAME") >>> ["one", "two", "three"]
//        db.getName() == qName
//        db.getTables().size() == 3
//        db.getTables() == expectedTables
    }

//    def "Can't get a Database Info on SQL exception"() {
//        def databaseName = UUID.randomUUID().toString()
//        def qName = QualifiedName.ofDatabase(UUID.randomUUID().toString(), databaseName)
//
//        when:
//        this.service.get(this.context, qName)
//
//        then:
//        1 * this.dataSource.getConnection() >> { throw new SQLException("blah") }
//        thrown RuntimeException
//    }

    def "Can list database Info's"() {
        def catalog = UUID.randomUUID().toString()
        def qName = QualifiedName.ofCatalog(UUID.randomUUID().toString())
        def schemaMetadata = Mock(DatabaseMetaData)
        def dbMetadata1 = Mock(DatabaseMetaData)
        def dbMetadata2 = Mock(DatabaseMetaData)
        def schemaResultSet = Mock(ResultSet)
//        def aDatabaseResultSet = Mock(ResultSet)
//        def bDatabaseResultSet = Mock(ResultSet)

        when:
        def databases = this.service.list(this.context, qName, null, null, null)

        then:
//        3 * this.dataSource.getConnection() >> this.connection
//        3 * this.connection.getMetaData() >>> [schemaMetadata, dbMetadata1, dbMetadata2]
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.getMetaData() >> schemaMetadata
        1 * schemaMetadata.getSchemas() >> schemaResultSet
        3 * schemaResultSet.next() >>> [true, true, false]
        2 * schemaResultSet.getString("TABLE_SCHEM") >>> ["a", "b"]
//        2 * this.connection.getCatalog() >> catalog
//        1 * dbMetadata1.getSearchStringEscape() >> "%"
//        1 * dbMetadata1.getTables(catalog, "a", "%", null) >> aDatabaseResultSet
//        4 * aDatabaseResultSet.next() >>> [true, true, true, false]
//        3 * aDatabaseResultSet.getString("TABLE_NAME") >>> ["one", "two", "three"]
//        1 * dbMetadata2.getSearchStringEscape() >> "%"
//        1 * dbMetadata2.getTables(catalog, "b", "%", null) >> bDatabaseResultSet
//        5 * bDatabaseResultSet.next() >>> [true, true, true, true, false]
//        4 * bDatabaseResultSet.getString("TABLE_NAME") >>> ["four", "five", "six", "seven"]
        databases.size() == 2
        databases.get(0).getName().getDatabaseName() == "a"
//        databases.get(0).getTables().size() == 3
//        databases.get(0).getTables() == ["one", "two", "three"]
        databases.get(1).getName().getDatabaseName() == "b"
//        databases.get(1).getTables().size() == 4
//        databases.get(1).getTables() == ["four", "five", "six", "seven"]
    }

    def "Can get list of database names without prefix, sort by or pagination"() {
        def qName = QualifiedName.ofCatalog(UUID.randomUUID().toString())
        def dbMetadata = Mock(DatabaseMetaData)
        def schemaResultSet = Mock(ResultSet)

        when:
        def databases = this.service.listNames(this.context, qName, null, null, null)

        then:
        1 * this.dataSource.getConnection() >> this.connection
        1 * this.connection.getMetaData() >> dbMetadata
        1 * dbMetadata.getSchemas() >> schemaResultSet
        6 * schemaResultSet.next() >>> [true, true, true, true, true, false]
        5 * schemaResultSet.getString("TABLE_SCHEM") >>> ["a", "b", "c", "d", "e"]
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
        1 * this.connection.getCatalog() >> UUID.randomUUID().toString()
        1 * this.connection.getMetaData() >> dbMetadata
        1 * dbMetadata.getSearchStringEscape() >> "%"
        1 * dbMetadata.getSchemas(_ as String, "a%") >> schemaResultSet
        4 * schemaResultSet.next() >>> [true, true, true, false]
        3 * schemaResultSet.getString("TABLE_SCHEM") >>> ["a", "ac", "ad"]
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
        1 * dbMetadata.getSchemas() >> schemaResultSet
        4 * schemaResultSet.next() >>> [true, true, true, false]
        3 * schemaResultSet.getString("TABLE_SCHEM") >>> ["a", "c", "b"]
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
        1 * dbMetadata.getSchemas() >> schemaResultSet
        4 * schemaResultSet.next() >>> [true, true, true, false]
        3 * schemaResultSet.getString("TABLE_SCHEM") >>> ["a", "c", "b"]
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
        1 * dbMetadata.getSchemas() >> schemaResultSet
        4 * schemaResultSet.next() >>> [true, true, true, false]
        3 * schemaResultSet.getString("TABLE_SCHEM") >>> ["a", "c", "b"]
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
        1 * dbMetadata.getSchemas() >> schemaResultSet
        4 * schemaResultSet.next() >>> [true, true, true, false]
        3 * schemaResultSet.getString("TABLE_SCHEM") >>> ["a", "c", "b"]
        databases.size() == 0
    }

    def "Renaming a database isn't supported"() {
        when:
        this.service.rename(
            this.context,
            QualifiedName.ofCatalog(UUID.randomUUID().toString()),
            QualifiedName.ofCatalog(UUID.randomUUID().toString())
        )

        then:
        thrown UnsupportedOperationException
    }
}
