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
package com.netflix.metacat.connector.postgresql

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.exception.*
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.sql.SQLException

/**
 * Tests for the PostgreSqlExceptionMapper.
 *
 * @author tgianos
 * @since 1.0.0
 */
class PostgreSqlExceptionMapperSpec extends Specification {

    @Shared
        name = QualifiedName.ofTable(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )

    @Unroll
    "Test to make sure #sqlException should throw #expected"() {
        def exceptionConverter = new PostgreSqlExceptionMapper()

        when:
        def connectorException = exceptionConverter.toConnectorException(sqlException, this.name)

        then:
        connectorException.getClass() == expected

        where:
        sqlException                                            | expected
        new SQLException("database exists", "42P04", 0)         | DatabaseAlreadyExistsException
        new SQLException("table exists", "42P07", 0)            | TableAlreadyExistsException
        new SQLException("database does not exist", "3D000", 0) | DatabaseNotFoundException
        new SQLException("database does not exist", "3F000", 0) | DatabaseNotFoundException
        new SQLException("table does not exist", "42P01", 0)    | TableNotFoundException
        new SQLException("Don't handle", "dummy", 0)            | ConnectorException
    }
}
