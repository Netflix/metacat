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
package com.netflix.metacat.connector.redshift

import com.google.common.collect.Lists
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper
import com.netflix.metacat.connector.jdbc.JdbcTypeConverter
import com.netflix.metacat.connector.jdbc.services.JdbcConnectorTableService
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.Statement

/**
 * Tests for the JdbcConnectorTableService APIs.
 *
 * @author tgianos
 * @since 1.0.0
 */
class RedshiftConnectorTableServiceSpec extends Specification {

    def context = Mock(ConnectorRequestContext)
    def dataSource = Mock(DataSource)
    def typeConverter = Mock(JdbcTypeConverter)
    def exceptionMapper = Mock(JdbcExceptionMapper)
    def service = new RedshiftConnectorTableService(this.dataSource, this.typeConverter, this.exceptionMapper)

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
        1 * statement.executeUpdate(
            "DROP TABLE " + qName.getCatalogName() + "." + qName.getDatabaseName() + "." + table
        )
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
        1 * statement.executeUpdate(
            "DROP TABLE " + qName.getCatalogName() + "." + qName.getDatabaseName() + "." + table
        )
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
        1 * statement.executeUpdate(
            "ALTER TABLE " + oldName.getDatabaseName() + ".c RENAME TO d"
        )
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
        1 * statement.executeUpdate(
            "ALTER TABLE " + oldName.getDatabaseName() + ".c RENAME TO d"
        )
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
