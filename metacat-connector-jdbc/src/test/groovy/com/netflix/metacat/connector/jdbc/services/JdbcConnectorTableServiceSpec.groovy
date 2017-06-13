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

import com.google.common.collect.Lists
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.exception.TableNotFoundException
import com.netflix.metacat.common.type.BaseType
import com.netflix.metacat.common.type.DecimalType
import com.netflix.metacat.common.type.VarcharType
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.Statement

/**
 * Tests for the JdbcConnectorTableService APIs.
 *
 * @author tgianos
 * @since 1.0.0
 */
class JdbcConnectorTableServiceSpec extends Specification {

    def context = Mock(ConnectorContext)
    def dataSource = Mock(DataSource)
    def typeConverter = Mock(JdbcTypeConverter)
    def exceptionMapper = Mock(JdbcExceptionMapper)
    def service = new JdbcConnectorTableService(this.dataSource, this.typeConverter, this.exceptionMapper)

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
        1 * connection.setSchema(database)
        1 * statement.executeUpdate("DROP TABLE " + table)
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
        1 * connection.setSchema(database)
        1 * statement.executeUpdate("DROP TABLE " + table)
    }

    def "Can get table metadata"() {
        def catalog = UUID.randomUUID().toString()
        def database = UUID.randomUUID().toString()
        def table = UUID.randomUUID().toString()
        def qName = QualifiedName.ofTable(catalog, database, table)
        def connection = Mock(Connection)
        def metadata = Mock(DatabaseMetaData)
        def columnResultSet = Mock(ResultSet)

        when:
        def tableInfo = this.service.get(this.context, qName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.getMetaData() >> metadata
        1 * metadata.getColumns(database, database, table, JdbcConnectorUtils.MULTI_CHARACTER_SEARCH) >> columnResultSet
        4 * columnResultSet.next() >>> [true, true, true, false]
        3 * columnResultSet.getString("REMARKS") >>> ["comment1", "comment2", "comment3"]
        3 * columnResultSet.getString("COLUMN_NAME") >>> ["column1", "column2", "column3"]
        3 * columnResultSet.getString("TYPE_NAME") >>> ["VARCHAR", "INTEGER", "DECIMAL"]
        3 * this.typeConverter.toMetacatType(_ as String) >>> [
            VarcharType.createVarcharType(255),
            BaseType.INT,
            DecimalType.createDecimalType(20, 10)
        ]
        3 * columnResultSet.getString("IS_NULLABLE") >>> ["YES", "NO", "NO"]
        3 * columnResultSet.getString("COLUMN_SIZE") >>> ["255", null, "20"]
        3 * columnResultSet.getString("DECIMAL_DIGITS") >>> [null, null, "10"]
        3 * columnResultSet.getString("COLUMN_DEF") >>> [null, "0", "0.0"]
        tableInfo.getName() == qName
        tableInfo.getFields().size() == 3
        tableInfo.getFields().get(0).getName() == "column1"
        tableInfo.getFields().get(1).getName() == "column2"
        tableInfo.getFields().get(2).getName() == "column3"
    }


    def "Cannot get table metadata"() {
        def catalog = UUID.randomUUID().toString()
        def database = UUID.randomUUID().toString()
        def table = UUID.randomUUID().toString()
        def qName = QualifiedName.ofTable(catalog, database, table)
        def connection = Mock(Connection)
        def metadata = Mock(DatabaseMetaData)
        def columnResultSet = Mock(ResultSet)
        def tablesResultSet = Mock(ResultSet)

        when:
        this.service.get(this.context, qName)

        then:
        thrown(TableNotFoundException)
        2 * this.dataSource.getConnection() >> connection
        2 * connection.getMetaData() >> metadata
        1 * metadata.getTables(database, database, table,_) >> tablesResultSet
        1 * tablesResultSet.next() >> false
        1 * metadata.getColumns(database, database, table, JdbcConnectorUtils.MULTI_CHARACTER_SEARCH) >> columnResultSet
        1 * columnResultSet.next() >> false
    }

    def "Can list table names without prefix, sort or pagination"() {
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
        1 * metadata.getTables(
            database,
            database,
            null,
            JdbcConnectorTableService.TABLE_TYPES
        ) >> tablesResultSet
        5 * tablesResultSet.next() >>> [true, true, true, true, false]
        4 * tablesResultSet.getString("TABLE_NAME") >>> ["a", "b", "c", "d"]
        tableNames.size() == 4
        tableNames.get(0).getTableName() == "a"
        tableNames.get(1).getTableName() == "b"
        tableNames.get(2).getTableName() == "c"
        tableNames.get(3).getTableName() == "d"
    }

    def "Can list table names with prefix but without sort or pagination"() {
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
        1 * metadata.getTables(
            database,
            database,
            prefix + JdbcConnectorUtils.MULTI_CHARACTER_SEARCH,
            JdbcConnectorTableService.TABLE_TYPES
        ) >> tablesResultSet
        5 * tablesResultSet.next() >>> [true, true, true, true, false]
        4 * tablesResultSet.getString("TABLE_NAME") >>> ["a", "b", "c", "d"]
        tableNames.size() == 4
        tableNames.get(0).getTableName() == "a"
        tableNames.get(1).getTableName() == "b"
        tableNames.get(2).getTableName() == "c"
        tableNames.get(3).getTableName() == "d"
    }

    def "Can list table names with sort but without prefix or pagination"() {
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
        1 * metadata.getTables(
            database,
            database,
            null,
            JdbcConnectorTableService.TABLE_TYPES
        ) >> tablesResultSet
        5 * tablesResultSet.next() >>> [true, true, true, true, false]
        4 * tablesResultSet.getString("TABLE_NAME") >>> ["a", "b", "c", "d"]
        tableNames.size() == 4
        tableNames.get(0).getTableName() == "d"
        tableNames.get(1).getTableName() == "c"
        tableNames.get(2).getTableName() == "b"
        tableNames.get(3).getTableName() == "a"
    }

    def "Can list table names with sort and pagination but without prefix"() {
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
        1 * metadata.getTables(
            database,
            database,
            null,
            JdbcConnectorTableService.TABLE_TYPES
        ) >> tablesResultSet
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
        1 * statement.executeUpdate("ALTER TABLE c RENAME TO d")
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
        1 * statement.executeUpdate("ALTER TABLE c RENAME TO d")
    }

    @Unroll
    "Can't call unsupported method #methodName"() {
        when:
        method.call()

        then:
        thrown exception

        where:
        method | methodName      | exception
        (
            {
                new JdbcConnectorTableService(
                    Mock(DataSource), Mock(JdbcTypeConverter), Mock(JdbcExceptionMapper)
                ).getTableNames(
                    Mock(ConnectorContext),
                    Lists.newArrayList(),
                    false
                )
            }
        )      | "getTableNames" | UnsupportedOperationException
        (
            {
                new JdbcConnectorTableService(
                    Mock(DataSource), Mock(JdbcTypeConverter), Mock(JdbcExceptionMapper)
                ).create(
                    Mock(ConnectorContext),
                    TableInfo.builder().name(QualifiedName.ofTable("catalog", "database", "table")).build()
                )
            }
        )      | "create"        | UnsupportedOperationException
        (
            {
                new JdbcConnectorTableService(
                    Mock(DataSource), Mock(JdbcTypeConverter), Mock(JdbcExceptionMapper)
                ).update(
                    Mock(ConnectorContext),
                    TableInfo.builder().name(QualifiedName.ofTable("catalog", "database", "table")).build()
                )
            }
        )      | "update"        | UnsupportedOperationException
    }

    @Unroll
    "Can build sourceType #expected from #type, #size, #precision"() {
        expect:
        this.service.buildSourceType(type, size, precision) == expected

        where:
        type                | size | precision | expected
        "int"               | null | null      | "int"
        "int unsigned"      | null | null      | "int unsigned"
        "smallint unsigned" | "5"  | null      | "smallint(5) unsigned"
        "double precision"  | "6"  | "10"      | "double precision(6, 10)"
        "INT Unsigned"      | "7"  | null      | "INT(7) Unsigned"
    }
}
