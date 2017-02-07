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
import com.netflix.metacat.common.server.connectors.model.TableInfo
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.*

/**
 * Tests for the JdbcConnectorTableService APIs.
 *
 * @author tgianos
 * @since 0.1.52
 */
class JdbcConnectorTableServiceSpec extends Specification {

    def context = Mock(ConnectorContext)
    def dataSource = Mock(DataSource)
    def service = new JdbcConnectorTableService(this.dataSource)

    def "Can't create a table"() {
        when:
        this.service.create(this.context, Mock(TableInfo))

        then:
        thrown UnsupportedOperationException
    }

    def "Can't update a table"() {
        when:
        this.service.update(this.context, Mock(TableInfo))

        then:
        thrown UnsupportedOperationException
    }

    def "Can delete an uppercase table"() {
        def connection = Mock(Connection)
        def statement = Mock(Statement)
        def metadata = Mock(DatabaseMetaData)

        def database = UUID.randomUUID().toString().toLowerCase()
        def table = UUID.randomUUID().toString().toLowerCase()
        def qName = QualifiedName.ofTable(UUID.randomUUID().toString(), database, table)

        when:
        this.service.delete(this.context, qName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.createStatement() >> statement
        1 * connection.getMetaData() >> metadata
        1 * metadata.storesUpperCaseIdentifiers() >> true
        1 * connection.setSchema(database.toUpperCase())
        1 * statement.executeUpdate("DROP TABLE " + table.toUpperCase() + ";")
    }

    def "Can delete a mixed case table"() {
        def connection = Mock(Connection)
        def statement = Mock(Statement)
        def metadata = Mock(DatabaseMetaData)

        def database = UUID.randomUUID().toString()
        def table = UUID.randomUUID().toString()
        def qName = QualifiedName.ofTable(UUID.randomUUID().toString(), database, table)

        when:
        this.service.delete(this.context, qName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.createStatement() >> statement
        1 * connection.getMetaData() >> metadata
        1 * metadata.storesUpperCaseIdentifiers() >> false
        1 * connection.setSchema(database)
        1 * statement.executeUpdate("DROP TABLE " + table + ";")
    }

    def "Can get table metadata"() {
        def catalog = UUID.randomUUID().toString()
        def database = UUID.randomUUID().toString()
        def table = UUID.randomUUID().toString()
        def qName = QualifiedName.ofTable(catalog, database, table)
        def escapeString = "%"
        def connection = Mock(Connection)
        def metadata = Mock(DatabaseMetaData)
        def columnResultSet = Mock(ResultSet)

        when:
        def tableInfo = this.service.get(this.context, qName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.getMetaData() >> metadata
        1 * connection.getCatalog() >> catalog
        1 * metadata.getSearchStringEscape() >> escapeString
        1 * metadata.getColumns(catalog, database, table, escapeString) >> columnResultSet
        3 * columnResultSet.next() >>> [true, true, false]
        2 * columnResultSet.getString("REMARKS") >>> ["comment1", "comment2"]
        2 * columnResultSet.getString("COLUMN_NAME") >>> ["column1", "column2"]
        2 * columnResultSet.getInt("ORDINAL_POSITION") >>> [1, 2]
        2 * columnResultSet.getString("TYPE_NAME") >>> ["VARCHAR", "INTEGER"]
        2 * columnResultSet.getInt("DATA_TYPE") >>> [Types.VARCHAR, Types.INTEGER]
        2 * columnResultSet.getString("IS_NULLABLE") >>> ["YES", "NO"]
        2 * columnResultSet.getInt("COLUMN_SIZE") >>> [255, 11]
        2 * columnResultSet.getString("COLUMN_DEF") >>> [null, "0"]
        tableInfo.getName() == qName
        tableInfo.getFields().size() == 2
        tableInfo.getFields().get(0).getName() == "column1"
        tableInfo.getFields().get(1).getName() == "column2"
    }

    def "Can list table names without prefix, sort or pagination"() {
        def escapeString = "%"
        def catalog = UUID.randomUUID().toString()
        def database = UUID.randomUUID().toString()
        def qName = QualifiedName.ofDatabase(catalog, database)
        def connection = Mock(Connection)
        def metadata = Mock(DatabaseMetaData)
        def tablesResultSet = Mock(ResultSet)

        when:
        def tableNames = this.service.listNames(this.context, qName, null, null, null)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.getMetaData() >> metadata
        1 * connection.getCatalog() >> catalog
        1 * metadata.getSearchStringEscape() >> escapeString
        1 * metadata.getTables(catalog, database, escapeString, null) >> tablesResultSet
        5 * tablesResultSet.next() >>> [true, true, true, true, false]
        4 * tablesResultSet.getString("TABLE_NAME") >>> ["a", "b", "c", "d"]
        tableNames.size() == 4
        tableNames.get(0).getTableName() == "a"
        tableNames.get(1).getTableName() == "b"
        tableNames.get(2).getTableName() == "c"
        tableNames.get(3).getTableName() == "d"
    }

    def "Can list table names with prefix but without sort or pagination"() {
        def escapeString = "%"
        def catalog = UUID.randomUUID().toString()
        def database = UUID.randomUUID().toString()
        def qName = QualifiedName.ofDatabase(catalog, database)
        def prefix = UUID.randomUUID().toString()
        def prefixQName = QualifiedName.ofTable(catalog, database, prefix)
        def connection = Mock(Connection)
        def metadata = Mock(DatabaseMetaData)
        def tablesResultSet = Mock(ResultSet)

        when:
        def tableNames = this.service.listNames(this.context, qName, prefixQName, null, null)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.getMetaData() >> metadata
        1 * connection.getCatalog() >> catalog
        1 * metadata.getSearchStringEscape() >> escapeString
        1 * metadata.getTables(catalog, database, prefix + escapeString, null) >> tablesResultSet
        5 * tablesResultSet.next() >>> [true, true, true, true, false]
        4 * tablesResultSet.getString("TABLE_NAME") >>> ["a", "b", "c", "d"]
        tableNames.size() == 4
        tableNames.get(0).getTableName() == "a"
        tableNames.get(1).getTableName() == "b"
        tableNames.get(2).getTableName() == "c"
        tableNames.get(3).getTableName() == "d"
    }

    def "Can list table names with sort but without prefix or pagination"() {
        def escapeString = "%"
        def catalog = UUID.randomUUID().toString()
        def database = UUID.randomUUID().toString()
        def qName = QualifiedName.ofDatabase(catalog, database)
        def connection = Mock(Connection)
        def metadata = Mock(DatabaseMetaData)
        def tablesResultSet = Mock(ResultSet)
        def sort = new Sort("blah", SortOrder.DESC)

        when:
        def tableNames = this.service.listNames(this.context, qName, null, sort, null)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.getMetaData() >> metadata
        1 * connection.getCatalog() >> catalog
        1 * metadata.getSearchStringEscape() >> escapeString
        1 * metadata.getTables(catalog, database, escapeString, null) >> tablesResultSet
        5 * tablesResultSet.next() >>> [true, true, true, true, false]
        4 * tablesResultSet.getString("TABLE_NAME") >>> ["a", "b", "c", "d"]
        tableNames.size() == 4
        tableNames.get(0).getTableName() == "d"
        tableNames.get(1).getTableName() == "c"
        tableNames.get(2).getTableName() == "b"
        tableNames.get(3).getTableName() == "a"
    }

    def "Can list table names with sort and pagination but without prefix"() {
        def escapeString = "%"
        def catalog = UUID.randomUUID().toString()
        def database = UUID.randomUUID().toString()
        def qName = QualifiedName.ofDatabase(catalog, database)
        def connection = Mock(Connection)
        def metadata = Mock(DatabaseMetaData)
        def tablesResultSet = Mock(ResultSet)
        def sort = new Sort("blah", SortOrder.DESC)
        def pageable = new Pageable(1, 2)

        when:
        def tableNames = this.service.listNames(this.context, qName, null, sort, pageable)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.getMetaData() >> metadata
        1 * connection.getCatalog() >> catalog
        1 * metadata.getSearchStringEscape() >> escapeString
        1 * metadata.getTables(catalog, database, escapeString, null) >> tablesResultSet
        5 * tablesResultSet.next() >>> [true, true, true, true, false]
        4 * tablesResultSet.getString("TABLE_NAME") >>> ["a", "b", "c", "d"]
        tableNames.size() == 1
        tableNames.get(0).getTableName() == "b"
    }

    def "Can't rename table if databases are different"() {
        def oldName = QualifiedName.ofTable("a", "b", "c")
        def newName = QualifiedName.ofTable("a", "d", "e")

        when:
        this.service.rename(this.context, oldName, newName)

        then:
        thrown IllegalArgumentException
    }

    def "Can rename uppercase table"() {
        def connection = Mock(Connection)
        def statement = Mock(Statement)
        def metadata = Mock(DatabaseMetaData)
        def oldName = QualifiedName.ofTable("a", "b", "c")
        def newName = QualifiedName.ofTable("a", "b", "d")

        when:
        this.service.rename(this.context, oldName, newName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.createStatement() >> statement
        1 * connection.getMetaData() >> metadata
        1 * metadata.storesUpperCaseIdentifiers() >> true
        1 * statement.executeUpdate("ALTER TABLE C RENAME D;")
    }

    def "Can rename lowercase table"() {
        def connection = Mock(Connection)
        def statement = Mock(Statement)
        def metadata = Mock(DatabaseMetaData)
        def oldName = QualifiedName.ofTable("a", "b", "c")
        def newName = QualifiedName.ofTable("a", "b", "d")

        when:
        this.service.rename(this.context, oldName, newName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.createStatement() >> statement
        1 * connection.getMetaData() >> metadata
        1 * metadata.storesUpperCaseIdentifiers() >> false
        1 * statement.executeUpdate("ALTER TABLE c RENAME d;")
    }
}
