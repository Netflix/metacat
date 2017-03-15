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
import com.google.common.collect.Lists
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.Pageable
import com.netflix.metacat.common.dto.Sort
import com.netflix.metacat.common.dto.SortOrder
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.connectors.model.FieldInfo
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.server.exception.DatabaseNotFoundException
import com.netflix.metacat.common.server.exception.TableNotFoundException
import com.netflix.metacat.common.type.ArrayType
import com.netflix.metacat.common.type.BaseType
import com.netflix.metacat.common.type.MapType
import com.netflix.metacat.common.type.VarbinaryType
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Specifications for the CassandraConnectorTableService implementation.
 *
 * @author tgianos
 * @since 1.0.0
 */
class CassandraConnectorTableServiceSpec extends Specification {

    @Shared
        catalog = UUID.randomUUID().toString()

    @Shared
        keyspace = UUID.randomUUID().toString()

    def context = Mock(ConnectorContext)
    def cluster = Mock(Cluster)
    def service = new CassandraConnectorTableService(this.cluster, new CassandraTypeConverter())

    def "Can Delete Table"() {
        def keyspace = UUID.randomUUID().toString()
        def table = UUID.randomUUID().toString()
        def name = QualifiedName.ofTable(this.catalog, keyspace, table)
        def session = Mock(Session)

        when:
        this.service.delete(this.context, name)

        then:
        1 * this.cluster.connect() >> session
        1 * session.execute("USE " + keyspace + "; DROP TABLE IF EXISTS " + table + ";")
        1 * session.close()
    }

    def "Can get Table"() {
        def table = UUID.randomUUID().toString()
        def name = QualifiedName.ofTable(this.catalog, this.keyspace, table)
        def metadata = Mock(Metadata)
        def keyspaceMetadata = Mock(KeyspaceMetadata)
        def tableMetadata = Mock(TableMetadata)
        def column1 = Mock(ColumnMetadata)
        def column2 = Mock(ColumnMetadata)
        def column3 = Mock(ColumnMetadata)

        when:
        def tableInfo = this.service.get(this.context, name)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspace(this.keyspace) >> keyspaceMetadata
        1 * keyspaceMetadata.getTable(table) >> tableMetadata
        1 * tableMetadata.getColumns() >> [column1, column2, column3]
        1 * tableMetadata.getName() >> table
        1 * column1.getName() >> "one"
        1 * column1.getType() >> DataType.list(DataType.cdouble())
        1 * column2.getName() >> "two"
        1 * column2.getType() >> DataType.map(DataType.text(), DataType.blob())
        1 * column3.getName() >> "three"
        1 * column3.getType() >> DataType.counter()
        tableInfo.getName() == name
        tableInfo.getFields().size() == 3
        tableInfo.getFields().get(0).getName() == "one"
        tableInfo.getFields().get(0).getType() == new ArrayType(BaseType.DOUBLE)
        tableInfo.getFields().get(1).getName() == "two"
        tableInfo.getFields().get(1).getType() == new MapType(
            BaseType.STRING,
            VarbinaryType.createVarbinaryType(Integer.MAX_VALUE)
        )
        tableInfo.getFields().get(2).getName() == "three"
        tableInfo.getFields().get(2).getType() == BaseType.BIGINT
    }

    @Unroll
    "Can't get Table"() {
        def table = UUID.randomUUID().toString()
        def name = QualifiedName.ofTable(this.catalog, this.keyspace, table)
        def metadata = Mock(Metadata)

        when:
        this.service.get(this.context, name)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspace(this.keyspace) >> keyspaceMetadata
        thrown exception

        where:
        keyspaceMetadata       | exception
        null                   | DatabaseNotFoundException
        Mock(KeyspaceMetadata) | TableNotFoundException
    }

    @Unroll
    "Can check if table exists"() {
        def table = UUID.randomUUID().toString()
        def name = QualifiedName.ofTable(this.catalog, this.keyspace, table)
        def metadata = Mock(Metadata)

        when:
        def exists = this.service.exists(this.context, name)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspace(this.keyspace) >> keyspaceMetadata
        exists == expected

        where:
        keyspaceMetadata | expected
        Mock(KeyspaceMetadata) {
            1 * getTable(_ as String) >> Mock(TableMetadata)
        }                | true
        Mock(KeyspaceMetadata) {
            1 * getTable(_ as String) >> null
        }                | false
        null             | false
    }

    @Unroll
    "Can list Tables"() {
        def name = QualifiedName.ofDatabase(this.catalog, this.keyspace)
        def metadata = Mock(Metadata)
        def keyspaceMetadata = Mock(KeyspaceMetadata)

        when:
        def result = this.service.list(this.context, name, prefix, sort, pageable)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspace(keyspace) >> keyspaceMetadata
        1 * keyspaceMetadata.getTables() >> tables
        result == expected

        where:
        tables | prefix                                              | pageable | sort | expected
        [
            Mock(TableMetadata) {
                1 * getColumns() >> [
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field1"
                        1 * getType() >> DataType.cint()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field2"
                        1 * getType() >> DataType.text()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field3"
                        1 * getType() >> DataType.cboolean()
                    }
                ]
                2 * getName() >> "aware"
            },
            Mock(TableMetadata) {
                1 * getColumns() >> [
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field1"
                        1 * getType() >> DataType.cdouble()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field2"
                        1 * getType() >> DataType.blob()
                    }
                ]
                2 * getName() >> "beware"
            },
            Mock(TableMetadata) {
                1 * getColumns() >> [
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field1"
                        1 * getType() >> DataType.cboolean()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field2"
                        1 * getType() >> DataType.smallint()
                    }
                ]
                2 * getName() >> "cold"
            }
        ]      |
            null                                                     |
            null                                                                |
            null                                                                       | [
            TableInfo.builder().name(QualifiedName.ofTable(this.catalog, this.keyspace, "aware")).fields([
                FieldInfo.builder().name("field1").type(BaseType.INT).sourceType("int").build(),
                FieldInfo.builder().name("field2").type(BaseType.STRING).sourceType("text").build(),
                FieldInfo.builder().name("field3").type(BaseType.BOOLEAN).sourceType("boolean").build()
            ]).build(),
            TableInfo.builder().name(QualifiedName.ofTable(this.catalog, this.keyspace, "beware")).fields([
                FieldInfo.builder().name("field1").type(BaseType.DOUBLE).sourceType("double").build(),
                FieldInfo.builder()
                    .name("field2")
                    .type(VarbinaryType.createVarbinaryType(Integer.MAX_VALUE))
                    .sourceType("blob")
                    .build()
            ]).build(),
            TableInfo.builder().name(QualifiedName.ofTable(this.catalog, this.keyspace, "cold")).fields([
                FieldInfo.builder().name("field1").type(BaseType.BOOLEAN).sourceType("boolean").build(),
                FieldInfo.builder().name("field2").type(BaseType.SMALLINT).sourceType("smallint").build(),
            ]).build()
        ]
        [
            Mock(TableMetadata) {
                1 * getColumns() >> [
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field1"
                        1 * getType() >> DataType.cint()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field2"
                        1 * getType() >> DataType.text()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field3"
                        1 * getType() >> DataType.cboolean()
                    }
                ]
                2 * getName() >> "aware"
            },
            Mock(TableMetadata) {
                0 * getColumns()
                1 * getName() >> "beware"
            },
            Mock(TableMetadata) {
                0 * getColumns()
                1 * getName() >> "cold"
            }
        ]      |
            QualifiedName.ofTable(this.catalog, this.keyspace, "aw") |
            null                                                                |
            null                                                                       | [
            TableInfo.builder().name(QualifiedName.ofTable(this.catalog, this.keyspace, "aware")).fields([
                FieldInfo.builder().name("field1").type(BaseType.INT).sourceType("int").build(),
                FieldInfo.builder().name("field2").type(BaseType.STRING).sourceType("text").build(),
                FieldInfo.builder().name("field3").type(BaseType.BOOLEAN).sourceType("boolean").build()
            ]).build()
        ]
        [
            Mock(TableMetadata) {
                1 * getColumns() >> [
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field1"
                        1 * getType() >> DataType.cint()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field2"
                        1 * getType() >> DataType.text()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field3"
                        1 * getType() >> DataType.cboolean()
                    }
                ]
                2 * getName() >> "aware"
            },
            Mock(TableMetadata) {
                1 * getColumns() >> [
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field1"
                        1 * getType() >> DataType.cdouble()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field2"
                        1 * getType() >> DataType.blob()
                    }
                ]
                2 * getName() >> "beware"
            },
            Mock(TableMetadata) {
                1 * getColumns() >> [
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field1"
                        1 * getType() >> DataType.cboolean()
                    },
                    Mock(ColumnMetadata) {
                        1 * getName() >> "field2"
                        1 * getType() >> DataType.smallint()
                    }
                ]
                2 * getName() >> "cold"
            }
        ]      |
            null                                                     |
            new Pageable(2, 0)                                                  |
            new Sort("blah", SortOrder.DESC)                                           | [
            TableInfo.builder().name(QualifiedName.ofTable(this.catalog, this.keyspace, "cold")).fields([
                FieldInfo.builder().name("field1").type(BaseType.BOOLEAN).sourceType("boolean").build(),
                FieldInfo.builder().name("field2").type(BaseType.SMALLINT).sourceType("smallint").build(),
            ]).build(),
            TableInfo.builder().name(QualifiedName.ofTable(this.catalog, this.keyspace, "beware")).fields([
                FieldInfo.builder().name("field1").type(BaseType.DOUBLE).sourceType("double").build(),
                FieldInfo.builder()
                    .name("field2")
                    .type(VarbinaryType.createVarbinaryType(Integer.MAX_VALUE))
                    .sourceType("blob")
                    .build()
            ]).build()
        ]
    }

    @Unroll
    "Can list Table Names"() {
        def name = QualifiedName.ofDatabase(this.catalog, this.keyspace)
        def metadata = Mock(Metadata)
        def keyspaceMetadata = Mock(KeyspaceMetadata)

        when:
        def result = this.service.listNames(this.context, name, prefix, sort, pageable)

        then:
        1 * this.cluster.getMetadata() >> metadata
        1 * metadata.getKeyspace(keyspace) >> keyspaceMetadata
        1 * keyspaceMetadata.getTables() >> tables
        result == expected

        where:
        tables | prefix                                               | pageable | sort | expected
        [
            Mock(TableMetadata) { 1 * getName() >> "aware" },
            Mock(TableMetadata) { 1 * getName() >> "beware" },
            Mock(TableMetadata) { 1 * getName() >> "cold" }
        ]      |
            null                                                      |
            null                                                                 |
            null                                                                        |
            [
                QualifiedName.ofTable(this.catalog, this.keyspace, "aware"),
                QualifiedName.ofTable(this.catalog, this.keyspace, "beware"),
                QualifiedName.ofTable(this.catalog, this.keyspace, "cold")
            ]
        [
            Mock(TableMetadata) { 1 * getName() >> "aware" },
            Mock(TableMetadata) { 1 * getName() >> "beware" },
            Mock(TableMetadata) { 1 * getName() >> "cold" }
        ]      |
            null                                                      |
            new Pageable(2, 0)                                                   |
            new Sort("blah", SortOrder.DESC)                                            |
            [
                QualifiedName.ofTable(this.catalog, this.keyspace, "cold"),
                QualifiedName.ofTable(this.catalog, this.keyspace, "beware")
            ]
        [
            Mock(TableMetadata) { 1 * getName() >> "aware" },
            Mock(TableMetadata) { 1 * getName() >> "beware" },
            Mock(TableMetadata) { 1 * getName() >> "cold" }
        ]      |
            QualifiedName.ofTable(this.catalog, this.keyspace, "col") |
            null                                                                 |
            null                                                                        |
            [
                QualifiedName.ofTable(this.catalog, this.keyspace, "cold")
            ]
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
                new CassandraConnectorTableService(Mock(Cluster), new CassandraTypeConverter()).create(
                    Mock(ConnectorContext),
                    TableInfo.builder().name(QualifiedName.ofTable("blah", "blah", "blah")).build()
                )
            }
        )      | "create"        | UnsupportedOperationException
        (
            {
                new CassandraConnectorTableService(Mock(Cluster), new CassandraTypeConverter()).update(
                    Mock(ConnectorContext),
                    TableInfo.builder().name(QualifiedName.ofTable("blah", "blah", "blah")).build()
                )
            }
        )      | "update"        | UnsupportedOperationException
        (
            {
                new CassandraConnectorTableService(Mock(Cluster), new CassandraTypeConverter()).rename(
                    Mock(ConnectorContext),
                    QualifiedName.ofTable("blah", "blah", "blah"),
                    QualifiedName.ofTable("blah", "blah", "blah2")
                )
            }
        )      | "rename" | UnsupportedOperationException
        (
            {
                new CassandraConnectorTableService(Mock(Cluster), new CassandraTypeConverter()).getTableNames(
                    Mock(ConnectorContext),
                    Lists.newArrayList(),
                    false
                )
            }
        )      | "getTableNames" | UnsupportedOperationException
    }
}
