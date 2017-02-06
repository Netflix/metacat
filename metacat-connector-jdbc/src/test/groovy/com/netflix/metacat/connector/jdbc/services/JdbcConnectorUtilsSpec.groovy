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

import spock.lang.Specification

import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
/**
 * Tests for JdbcConnectorUtils methods.
 *
 * @author tgianos
 * @since 0.1.52
 */
class JdbcConnectorUtilsSpec extends Specification {

    def "Can execute an update"() {
        def sql = "CREATE TABLE TEST;"
        def connection = Mock(Connection)
        def statement = Mock(Statement)

        when: "When execute an update"
        def updated = JdbcConnectorUtils.executeUpdate(connection, sql)

        then: "Should return 1"
        1 * connection.createStatement() >> statement
        1 * statement.executeUpdate(sql) >> 1
        updated == 1
    }

    def "Can't execute an update"() {
        def sql = "CREATE TABLE TEST;"
        def connection = Mock(Connection)

        when: "When execute an update"
        JdbcConnectorUtils.executeUpdate(connection, sql)

        then: "Will propagate a runtime exception"
        1 * connection.createStatement() >> { throw new SQLException("oops") }
        thrown RuntimeException
    }
}
