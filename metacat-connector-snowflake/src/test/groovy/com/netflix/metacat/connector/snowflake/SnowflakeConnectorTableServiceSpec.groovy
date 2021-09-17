/*
 *
 * Copyright 2018 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */
package com.netflix.metacat.connector.snowflake

import com.google.common.collect.Lists
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.exception.TableNotFoundException
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorTableService
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

/**
 * Tests for the JdbcConnectorTableService APIs.
 *
 * @author amajumdar
 * @since 1.2.0
 */
class SnowflakeConnectorTableServiceSpec extends Specification {

    def context = Mock(ConnectorRequestContext)
    def dataSource = Mock(DataSource)
    def typeConverter = Mock(JdbcTypeConverter)
    def exceptionMapper = new SnowflakeExceptionMapper()
    def service = new SnowflakeConnectorTableService(this.dataSource, this.typeConverter, this.exceptionMapper)

    def "Can delete an uppercase table"() {
        def connection = Mock(Connection)
        def statement = Mock(Statement)

        def database = UUID.randomUUID().toString().toUpperCase()
        def table = UUID.randomUUID().toString().toUpperCase()
        def qName = QualifiedName.ofTable(UUID.randomUUID().toString(), database, table)

        when:
        this.service.delete(this.context, qName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.createStatement() >> statement
        1 * connection.getCatalog() >> "dse"
        1 * connection.setSchema(_)
        1 * statement.executeUpdate(
            "DROP TABLE " + table
        )
    }

    def "Can delete a mixed case table"() {
        def connection = Mock(Connection)
        def statement = Mock(Statement)

        def database = UUID.randomUUID().toString().toUpperCase()
        def table = UUID.randomUUID().toString().toUpperCase()
        def qName = QualifiedName.ofTable(UUID.randomUUID().toString(), database, table)

        when:
        this.service.delete(this.context, qName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.createStatement() >> statement
        1 * connection.getCatalog() >> "dse"
        1 * connection.setSchema(_)
        1 * statement.executeUpdate(
            "DROP TABLE " + table
        )
    }

    def "get table"() {
        def connection = Mock(Connection)
        def metadata = Mock(DatabaseMetaData)
        def resultset = Mock(ResultSet)
        def resultset1 = Mock(ResultSet)
        dataSource.getConnection() >> connection
        connection.getMetaData() >> metadata
        def database = UUID.randomUUID().toString().toUpperCase()
        def table = UUID.randomUUID().toString().toUpperCase()
        def qName = QualifiedName.ofTable(UUID.randomUUID().toString(), database, table)

        when:
        this.service.get(context, qName)

        then:
        noExceptionThrown()
        1 * metadata.getColumns(_,_,_,_) >> resultset
        1 * resultset.next() >> false
        1 * metadata.getTables(_,_,_,_) >> resultset1
        1 * resultset1.next() >> true

        when:
        this.service.get(context, qName)

        then:
        thrown(TableNotFoundException)
        1 * metadata.getColumns(_,_,_,_) >> {throw new SQLException("reason", "42SO2", 2003)}
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
        def oldName = QualifiedName.ofTable("a", "b", "c")
        def newName = QualifiedName.ofTable("a", "b", "d")

        when:
        this.service.rename(this.context, oldName, newName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.createStatement() >> statement
        1 * connection.getCatalog() >> "dse"
        2 * connection.setSchema(_)
        1 * statement.executeUpdate(
            "ALTER TABLE C RENAME TO D"
        )
    }

    def "Can rename lowercase table"() {
        def connection = Mock(Connection)
        def statement = Mock(Statement)
        def oldName = QualifiedName.ofTable("a", "b", "c")
        def newName = QualifiedName.ofTable("a", "b", "d")

        when:
        this.service.rename(this.context, oldName, newName)

        then:
        1 * this.dataSource.getConnection() >> connection
        1 * connection.createStatement() >> statement
        1 * connection.getCatalog() >> "dse"
        2 * connection.setSchema(_)
        1 * statement.executeUpdate(
            "ALTER TABLE C RENAME TO D"
        )
    }

    def "set table details"() {
        given:
        def connection = Mock(Connection)
        def statement = Mock(PreparedStatement)
        def rs = Mock(ResultSet)
        def name = QualifiedName.ofTable("a", "b", "c")
        def date = new Date(1,1,1)
        when:
        def table = TableInfo.builder().name(name).build()
        this.service.setTableInfoDetails(connection, table)
        then:
        1 * connection.prepareStatement(_) >> statement
        1 * statement.executeQuery() >> rs
        1 * rs.next() >> true
        2 * rs.getDate(_) >> date
        table.getAudit().getCreatedDate() != null
        when:
        this.service.setTableInfoDetails(connection, table)
        then:
        1 * connection.prepareStatement(_) >> statement
        1 * statement.executeQuery() >> {throw new SQLException()}
        noExceptionThrown()
        when:
        table = TableInfo.builder().name(name).build()
        this.service.setTableInfoDetails(connection, table)
        then:
        1 * connection.prepareStatement(_) >> statement
        1 * statement.executeQuery() >> rs
        1 * rs.next() >> false
        table.getAudit() == null
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
                    Mock(ConnectorRequestContext),
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
                    Mock(ConnectorRequestContext),
                    TableInfo.builder().name(QualifiedName.ofTable("catalog", "database", "table")).build()
                )
            }
        )      | "create"        | UnsupportedOperationException
        (
            {
                new JdbcConnectorTableService(
                    Mock(DataSource), Mock(JdbcTypeConverter), Mock(JdbcExceptionMapper)
                ).update(
                    Mock(ConnectorRequestContext),
                    TableInfo.builder().name(QualifiedName.ofTable("catalog", "database", "table")).build()
                )
            }
        )      | "update"        | UnsupportedOperationException
    }
}
