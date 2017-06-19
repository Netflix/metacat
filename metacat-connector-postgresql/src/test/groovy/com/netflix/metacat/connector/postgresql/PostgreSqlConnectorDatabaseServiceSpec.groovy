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
import com.netflix.metacat.common.server.connectors.ConnectorRequestContext
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import com.netflix.metacat.connector.jdbc.JdbcExceptionMapper
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource

/**
 * Specifications for the PostgreSQL Connector Database Service.
 *
 * @author tgianos
 * @since 1.0.0
 */
class PostgreSqlConnectorDatabaseServiceSpec extends Specification {


    @Unroll
    "Can't call unsupported method #methodName"() {
        when:
        method.call()

        then:
        thrown exception

        where:
        method | methodName | exception
        (
            {
                new PostgreSqlConnectorDatabaseService(Mock(DataSource), Mock(JdbcExceptionMapper)).create(
                    Mock(ConnectorRequestContext),
                    DatabaseInfo.builder().name(QualifiedName.ofDatabase("blah", "db")).build()
                )
            }
        )      | "create"   | UnsupportedOperationException
        (
            {
                new PostgreSqlConnectorDatabaseService(Mock(DataSource), Mock(JdbcExceptionMapper)).delete(
                    Mock(ConnectorRequestContext),
                    QualifiedName.ofDatabase("blah", "db")
                )
            }
        )      | "delete"   | UnsupportedOperationException
    }
}
