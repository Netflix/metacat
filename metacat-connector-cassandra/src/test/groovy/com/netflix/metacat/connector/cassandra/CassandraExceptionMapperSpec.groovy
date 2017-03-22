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
package com.netflix.metacat.connector.cassandra

import com.datastax.driver.core.exceptions.AlreadyExistsException
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.exception.DatabaseAlreadyExistsException
import com.netflix.metacat.common.server.exception.TableAlreadyExistsException
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for the CassandraExceptionMapper.
 *
 * @author tgianos
 * @since 1.0.0
 */
class CassandraExceptionMapperSpec extends Specification {

    @Shared
        mapper = new CassandraExceptionMapper()
    @Shared
        table = QualifiedName.ofTable(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )
    @Shared
        database = QualifiedName.ofDatabase(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        )

    @Unroll
    "Throws #expected for driver exception #input"() {

        expect:
        this.mapper.toConnectorException(input, name).class == expected

        where:
        input                                           | name          | expected
        new AlreadyExistsException("keyspace", "")      | this.database | DatabaseAlreadyExistsException
        new AlreadyExistsException("keyspace", "table") | this.table    | TableAlreadyExistsException
    }
}
