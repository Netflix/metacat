/*
 * Copyright 2016 Netflix, Inc.
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.metacat.converters.impl

import com.facebook.presto.metadata.QualifiedTableName
import com.facebook.presto.metadata.TableMetadata
import com.facebook.presto.spi.*
import com.facebook.presto.spi.type.BigintType
import com.facebook.presto.spi.type.TypeManager
import com.facebook.presto.spi.type.VarcharType
import com.facebook.presto.type.TypeRegistry
import com.google.inject.Provider
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.server.Config
import com.netflix.metacat.converters.TypeConverter
import de.danielbechler.diff.ObjectDiffer
import de.danielbechler.diff.ObjectDifferBuilder
import de.danielbechler.diff.node.DiffNode
import de.danielbechler.diff.node.Visit
import org.mapstruct.factory.Mappers
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.time.LocalDateTime
import java.time.Month
import java.time.ZoneId
import java.time.ZoneOffset

class PrestoConvertersSpec extends Specification {
    private static final Logger log = LoggerFactory.getLogger(PrestoConvertersSpec)
    private static final ZoneOffset PACIFIC = LocalDateTime.now().atZone(ZoneId.of('America/Los_Angeles')).offset

    @Shared
    TypeManager typeManager = new TypeRegistry()
    @Shared
    MapStructPrestoConverters converter
    @Shared
    Config config = Mock(Config)

    def setupSpec(){
        // Stub this to always return true
        config.isEpochInSeconds() >> true
        DateConverters.setConfig(config)
        converter = Mappers.getMapper(MapStructPrestoConverters.class)
        converter.setTypeConverter( new Provider<TypeConverter>() {
            @Override
            TypeConverter get() {
                return new PrestoTypeConverter()
            }
        })
    }

    boolean objectsSame(def actual, def expected) {
        ObjectDifferBuilder builder = ObjectDifferBuilder.startBuilding()
        builder.inclusion()
                .exclude().propertyNameOfType(TableDto, 'dataUri').also()
                .exclude().propertyNameOfType(PartitionDto, 'dataUri').also()
                .exclude().propertyNameOfType(QualifiedName, 'databaseName', 'tableName', 'partitionName')

        ObjectDiffer differ = builder.build()
        DiffNode diff = differ.compare(actual, expected)
        if (diff.hasChanges()) {
            diff.visit({ DiffNode node, Visit visit ->
                log.info("DIFFERENCE {} => {}", node.getPath(), node.getState())
            } as DiffNode.Visitor)
        }
        return !diff.hasChanges()
    }

    TableDto getDummyTableDto(int i) {
        if (i == 0) {
            return new TableDto(
                    audit: new AuditDto(
                            createdBy: 'dwatson',
                            createdDate: Date.from(LocalDateTime.of(2015, Month.APRIL, 8, 4, 33, 47).toInstant(PACIFIC)),
                            lastModifiedBy: 'amajumdar',
                            lastModifiedDate: Date.from(LocalDateTime.of(2015, Month.APRIL, 8, 4, 51, 12).toInstant(PACIFIC)),
                    ),
                    dataMetadata: null,
                    definitionMetadata: null,
                    fields: [
                            new FieldDto(
                                    comment: null,
                                    name: 'c1',
                                    partition_key: true,
                                    pos: 0,
                                    source_type: null,
                                    type: 'bigint'
                            ),
                            new FieldDto(
                                    comment: null,
                                    name: 'c2',
                                    partition_key: true,
                                    pos: 1,
                                    source_type: null,
                                    type: 'varchar'
                            )
                    ],
                    metadata: ['mdc1': 'mdv1'],
                    name: QualifiedName.fromString('catalog/db/table'),
                    serde: new StorageDto(
                            inputFormat: 'siIF',
                            outputFormat: 'siOF',
                            owner: 'owner',
                            parameters: ['sik1': 'siv1'],
                            serdeInfoParameters: ['sipk1': 'sipv1'],
                            serializationLib: 'siSL',
                            uri: 'siUri'
                    )
            )
        }
    }

    TableMetadata getDummyTableMetadata(int i) {
        if (i == 0) {
            return new TableMetadata('catalog', new ConnectorTableDetailMetadata(
                    new SchemaTableName('db', 'table'),
                    [
                            new ColumnMetadata('c1', BigintType.BIGINT, true),
                            new ColumnMetadata('c2', VarcharType.VARCHAR, true)
                    ],
                    'owner',
                    new StorageInfo('siUri', 'siIF', 'siOF', 'siSL', ['sik1': 'siv1'], ['sipk1': 'sipv1']),
                    ['mdc1': 'mdv1'],
                    new AuditInfo(
                            'dwatson',
                            'amajumdar',
                            LocalDateTime.of(2015, Month.APRIL, 8, 4, 33, 47).toInstant(PACIFIC).getEpochSecond(),
                            LocalDateTime.of(2015, Month.APRIL, 8, 4, 51, 12).toInstant(PACIFIC).getEpochSecond()
                    )
            ))
        }
    }

    PartitionDto getDummyPartitionDto(int i) {
        if (i == 0) {
            return new PartitionDto(
                    audit: new AuditDto(
                            createdBy: 'dwatson',
                            createdDate: Date.from(LocalDateTime.of(2015, Month.APRIL, 8, 4, 33, 47).toInstant(PACIFIC)),
                            lastModifiedBy: 'amajumdar',
                            lastModifiedDate: Date.from(LocalDateTime.of(2015, Month.APRIL, 8, 4, 51, 12).toInstant(PACIFIC)),
                    ),
                    dataMetadata: null,
                    definitionMetadata: null,
                    name: QualifiedName.fromString('catalog/db/table/partition'),
                    serde: new StorageDto(
                            inputFormat: 'siIF',
                            outputFormat: 'siOF',
                            owner: 'dwatson',
                            parameters: ['sik1': 'siv1'],
                            serdeInfoParameters: ['sipk1': 'sipv1'],
                            serializationLib: 'siSL',
                            uri: 'siUri'
                    )
            )
        }

    }

    ConnectorPartition getDummyPrestoPartition(int i) {
        if (i == 0) {
            return new ConnectorPartitionDetailImpl(
                    'partition',
                    TupleDomain.none(),
                    new StorageInfo('siUri', 'siIF', 'siOF', 'siSL', ['sik1': 'siv1'], ['sipk1': 'sipv1']),
                    null,
                    new AuditInfo(
                            'dwatson',
                            'amajumdar',
                            LocalDateTime.of(2015, Month.APRIL, 8, 4, 33, 47).toInstant(PACIFIC).getEpochSecond(),
                            LocalDateTime.of(2015, Month.APRIL, 8, 4, 51, 12).toInstant(PACIFIC).getEpochSecond()
                    )
            )
        }

    }

    def "make sure it is possible to get a converters instance"() {
        expect: "Expect converter to be created - if it is not the conversion has java errors in the source generation"
        converter
    }

    @Unroll
    def "test converting '#name' to a QualifiedTableName"() {
        given:
        def actualResult = null

        when:
        try {
            actualResult = converter.getQualifiedTableName(name)
        } catch (Throwable t) {
            actualResult = t
        }

        then:
        if (expectedResult instanceof Exception) {
            assert actualResult.class == expectedResult.class
        } else {
            actualResult == expectedResult
        }

        where:
        name                                                         | expectedResult
        null                                                         | new NullPointerException('name cannot be null')
        QualifiedName.fromString('catalog')                          | new IllegalStateException('Not a table definition: catalog')
        QualifiedName.fromString('catalog/database')                 | new IllegalStateException('Not a table definition: catalog/database')
        QualifiedName.fromString('catalog/database/table/partition') | new QualifiedTableName('catalog', 'database', 'table')
        QualifiedName.fromString('catalog/database/table')           | new QualifiedTableName('catalog', 'database', 'tablez')
    }

    @Unroll
    def "can convert a presto table for '#name' into our TableDTO"() {
        given:

        expect:
        def actualDto = converter.toTableDto(name, type, prestoTM)
        objectsSame(actualDto, expectedDto)

        where:
        expectedDto         | name                                         | type | prestoTM
        null                | null                                         | null | null
        getDummyTableDto(0) | QualifiedName.fromString('catalog/db/table') | null | getDummyTableMetadata(0)
    }

    def "can convert a QualifiedTableName to a QualifiedName"() {
        expect:
        QualifiedName.fromString('catalog/db/table') == converter.toQualifiedName(new QualifiedTableName('catalog', 'db', 'table'))
    }

    @Unroll
    def "can convert our tableDto into a presto table for '#name'"() {
        given:

        expect:
        def actualPrestoTM = converter.fromTableDto(name, tableDto, type)
        objectsSame(actualPrestoTM, expectedPrestoTM)

        where:
        expectedPrestoTM         | name                                         | type        | tableDto
        getDummyTableMetadata(0) | QualifiedName.fromString('catalog/db/table') | typeManager | getDummyTableDto(0)
    }

    @Unroll
    def "can convert a presto partition into our PartitionDto"() {
        given:

        expect:
        def actualDto = converter.toPartitionDto(name, prestoPartition)
        objectsSame(actualDto, expectedDto)

        where:
        expectedDto             | name                                                   | prestoPartition
        getDummyPartitionDto(0) | QualifiedName.fromString('catalog/db/table/partition') | getDummyPrestoPartition(0)
    }
}
