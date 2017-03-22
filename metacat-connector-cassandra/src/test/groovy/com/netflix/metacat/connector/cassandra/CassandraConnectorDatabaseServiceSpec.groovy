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

import com.datastax.driver.core.*
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for the CassandraConnectorDatabaseService implementation.
 *
 * @author tgianos
 * @since 1.0.0
 */
class CassandraConnectorDatabaseServiceSpec extends Specification {

    @Shared
        catalogName = UUID.randomUUID().toString()
    @Shared
        a = "always"
    @Shared
        b = "better"
    @Shared
        c = "cassandra"
    @Shared
        aQName = QualifiedName.ofDatabase(this.catalogName, this.a)
    @Shared
        bQName = QualifiedName.ofDatabase(this.catalogName, this.b)
    @Shared
        cQName = QualifiedName.ofDatabase(this.catalogName, this.c)

    def context = Mock(ConnectorContext)
    def cluster = Mock(Cluster)
    def session = Mock(Session)
    def service = new CassandraConnectorDatabaseService(this.cluster, new CassandraExceptionMapper())

    def "Can create a Keyspace"() {
        def keyspace = UUID.randomUUID().toString()
        def name = QualifiedName.ofDatabase(UUID.randomUUID().toString(), keyspace)
        def resource = DatabaseInfo.builder().name(name).build()

        when:
        this.service.create(this.context, resource)

        then:
        1 * this.cluster.connect() >> this.session
        1 * this.session.execute(
            "CREATE KEYSPACE "
                + keyspace
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};"
        )
        1 * this.session.close()
    }

    def "Can drop a Keyspace"() {
        def keyspace = UUID.randomUUID().toString()
        def name = QualifiedName.ofDatabase(UUID.randomUUID().toString(), keyspace)

        when:
        this.service.delete(this.context, name)

        then:
        1 * this.cluster.connect() >> this.session
        1 * this.session.execute("DROP KEYSPACE IF EXISTS " + keyspace + ";")
        1 * this.session.close()
    }

    def "Can get a Keyspace"() {
        def metadata = Mock(Metadata)

        when:
        def database = this.service.get(this.context, this.aQName)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspace(this.a) >> Mock(KeyspaceMetadata)
        database.getName() == this.aQName
    }

    def "Can't get a Keyspace"() {
        def metadata = Mock(Metadata)

        when:
        this.service.get(this.context, this.aQName)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspace(this.a) >> null
        thrown DatabaseNotFoundException
    }

    def "Can get view names"() {
        def metadata = Mock(Metadata)
        def keyspaceMetadata = Mock(KeyspaceMetadata)
        def tableMetadata1 = Mock(TableMetadata)
        def tableMetadata2 = Mock(TableMetadata)
        def view1 = Mock(MaterializedViewMetadata)
        def view2 = Mock(MaterializedViewMetadata)
        def view3 = Mock(MaterializedViewMetadata)

        when:
        def views = this.service.listViewNames(this.context, this.aQName)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspace(this.a) >> keyspaceMetadata
        1 * keyspaceMetadata.getMaterializedViews() >> [view1, view2, view3]
        1 * view1.getBaseTable() >> tableMetadata1
        1 * view2.getBaseTable() >> tableMetadata1
        1 * view3.getBaseTable() >> tableMetadata2
        2 * tableMetadata1.getName() >> "1"
        1 * tableMetadata2.getName() >> "2"
        1 * view1.getName() >> "1"
        1 * view2.getName() >> "2"
        1 * view3.getName() >> "3"
        views == [
            QualifiedName.ofView(this.catalogName, this.a, "1", "1"),
            QualifiedName.ofView(this.catalogName, this.a, "1", "2"),
            QualifiedName.ofView(this.catalogName, this.a, "2", "3"),
        ]
    }

    @Unroll
    "Can see if a Keyspace Exists"() {
        def keyspace = UUID.randomUUID().toString()
        def name = QualifiedName.ofDatabase(UUID.randomUUID().toString(), keyspace)
        def metadata = Mock(Metadata)

        when:
        def exists = this.service.exists(this.context, name)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspace(keyspace) >> keyspaceMetadata
        exists == expectedResult

        where:
        keyspaceMetadata       | expectedResult
        Mock(KeyspaceMetadata) | true
        null                   | false
    }

    @Unroll
    "Can list Keyspace names"() {
        def name = QualifiedName.ofCatalog(this.catalogName)
        def keySpaceA = Mock(KeyspaceMetadata)
        def keySpaceB = Mock(KeyspaceMetadata)
        def keySpaceC = Mock(KeyspaceMetadata)
        def keyspaces = [keySpaceA, keySpaceB, keySpaceC]
        def metadata = Mock(Metadata)

        when:
        def results = this.service.listNames(this.context, name, prefix, sort, pageable)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspaces() >> keyspaces
        1 * keySpaceA.getName() >> this.a
        1 * keySpaceB.getName() >> this.b
        1 * keySpaceC.getName() >> this.c
        results == expected

        where:
        prefix | sort                          | pageable           | expected
        QualifiedName.ofDatabase(
            this.catalogName,
            "cas"
        )      | null                          | null               | [this.cQName]
        QualifiedName.ofDatabase(
            this.catalogName,
            UUID.randomUUID().toString()
        )      | null                          | null               | []
        null   | null                          | null               | [this.aQName, this.bQName, this.cQName]
        null   | new Sort("a", SortOrder.DESC) | null               | [this.cQName, this.bQName, this.aQName]
        null   | new Sort("a", SortOrder.ASC)  | null               | [this.aQName, this.bQName, this.cQName]
        null   | null                          | new Pageable()     | [this.aQName, this.bQName, this.cQName]
        null   | null                          | new Pageable(2, 1) | [this.bQName, this.cQName]
    }

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
                new CassandraConnectorDatabaseService(Mock(Cluster), new CassandraExceptionMapper())
                    .rename(Mock(ConnectorContext), QualifiedName.ofCatalog("blah"), QualifiedName.ofCatalog("junk"))
            }
        )      | "rename"   | UnsupportedOperationException
        (
            {
                new CassandraConnectorDatabaseService(Mock(Cluster), new CassandraExceptionMapper()).update(
                    Mock(ConnectorContext),
                    DatabaseInfo.builder().name(QualifiedName.ofCatalog("blah")).build()
                )
            }
        )      | "update"   | UnsupportedOperationException
    }
}
