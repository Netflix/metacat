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

package com.netflix.metacat.thrift

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.server.connectors.ConnectorTypeConverter
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.type.VarcharType
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.*
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.joda.time.Instant
import spock.lang.Specification

import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Date

class HiveConvertersSpec extends Specification {
    private static final ZoneOffset PACIFIC = LocalDateTime.now().atZone(ZoneId.of('America/Los_Angeles')).offset
    Config config = Mock(Config)
//    TypeManager typeManager = Mock(TypeManager)
    HiveConvertersImpl converter
    ConnectorTypeConverter hiveTypeConverter = Mock(ConnectorTypeConverter)

    def setup() {
        // Stub this to always return true
        config.isEpochInSeconds() >> true
//        DateConverters.setConfig(config)
        converter = new HiveConvertersImpl()
    }

    def 'test date to epoch seconds'() {
        when:
        def result = converter.dateToEpochSeconds(input)

        then:
        result == output

        where:
        input                                         | output
        null                                          | null
        Instant.parse('2016-02-25T14:47:27').toDate() | Instant.parse('2016-02-25T14:47:27').getMillis() / 1000
    }

    def 'test date to epoch seconds failure'() {
        when:
        converter.dateToEpochSeconds(input)

        then:
        thrown(IllegalStateException)

        where:
        input = Date.from(LocalDateTime.of(9999, 2, 25, 14, 47, 27).toInstant(PACIFIC))
    }

    def 'test metacatToHiveDatabase sets all required fields to non-null values'() {
        given:
        def dto = new DatabaseDto()

        when:
        def db = converter.metacatToHiveDatabase(dto)

        then:
        db
        db.name != null
        db.description != null
        db.locationUri != null
        db.parameters != null
    }

    def 'test metacatToHiveDatabase'() {
        given:
        def dto = new DatabaseDto(
            name: QualifiedName.ofDatabase('catalog', databaseName),
            uri: dbUri,
            metadata: metadata
        )

        when:
        def db = converter.metacatToHiveDatabase(dto)

        then:
        db
        db.name == databaseName
        db.description == databaseName
        db.locationUri == dbUri
        db.parameters == metadata

        where:
        databaseName = 'db'
        dbUri = 'http://example.com'
        metadata = [k1: 'v1', k2: 'v2']
    }

    def 'test metacatToHiveTable sets all required fields to non-null values'() {
        given:
        TableDto dto = new TableDto()

        when:
        def table = converter.metacatToHiveTable(dto)

        then:
        table
        table.tableName == null
        table.dbName == null
        table.owner == null
        table.sd != null
        table.sd.cols != null
        table.sd.location == null
        table.sd.inputFormat == null
        table.sd.outputFormat == null
        table.sd.serdeInfo != null
        table.sd.serdeInfo.name == null
        table.sd.serdeInfo.serializationLib == null
        table.sd.serdeInfo.parameters != null
        table.sd.bucketCols != null
        table.sd.sortCols != null
        table.sd.parameters != null
        table.partitionKeys != null
        table.parameters != null
        table.tableType != null
    }

    def 'test metacatToHiveTable'() {
        when:
        def table = converter.metacatToHiveTable(dto)

        then:
        table.dbName == databaseName
        table.tableName == tableName
        table.owner == owner
        table.createTime == Instant.parse('2016-02-25T14:47:27').getMillis() / 1000
        table.sd.cols.size() == fields.size() - 2
        table.sd.cols.each {
            it.name.startsWith('field_')
            it.comment.startsWith('comment_')
            it.type == 'VARCHAR'
        }
        table.sd.location == location
        table.sd.inputFormat == inputFormat
        table.sd.outputFormat == outputFormat
        table.sd.serdeInfo.name == tableName
        table.sd.serdeInfo.serializationLib == serializationLib
        table.sd.serdeInfo.parameters == serdeInfoParams
        table.sd.parameters == storageParams
        table.partitionKeys.size() == 2
        table.partitionKeys.each {
            it.name.startsWith('field_')
            it.comment.startsWith('comment_')
            it.type == 'VARCHAR'
        }
        table.parameters == tableParams
        table.tableType == 'EXTERNAL_TABLE'

        where:
        databaseName = 'database'
        tableName = 'table'
        owner = 'owner'
        createDate = Instant.parse('2016-02-25T14:47:27').toDate()
        location = 'location'
        inputFormat = 'inputFormat'
        outputFormat = 'outputFormat'
        serializationLib = 'serializationLib'
        storageParams = ['sk1': 'sv1']
        serdeInfoParams = ['sik1': 'siv1']
        fields = (0..9).collect {
            new FieldDto(name: "field_$it", partition_key: it < 2, comment: "comment_$it", type: 'VARCHAR')
        }
        tableParams = ['tk1': 'tv1']
        dto = new TableDto(
            name: QualifiedName.ofTable('catalog', databaseName, tableName),
            audit: new AuditDto(createdDate: createDate),
            serde: new StorageDto(
                owner: owner,
                uri: location,
                inputFormat: inputFormat,
                outputFormat: outputFormat,
                serializationLib: serializationLib,
                parameters: storageParams,
                serdeInfoParameters: serdeInfoParams
            ),
            fields: fields,
            metadata: tableParams
        )
    }

    def 'test hiveToMetacatTable'() {
        when:
        def dto = converter.hiveToMetacatTable(name, table)

        then:
        hiveTypeConverter.toMetacatType('VARCHAR') >> VarcharType.VARCHAR
        dto.name == name
        dto.name.databaseName == databaseName
        dto.name.tableName == tableName
        dto.audit.createdDate == Instant.parse('2016-02-25T14:47:27').toDate()
        dto.serde.owner == owner
        dto.serde.uri == location
        dto.serde.inputFormat == inputFormat
        dto.serde.outputFormat == outputFormat
        dto.serde.serializationLib == serializationLib
        dto.serde.serdeInfoParameters == serdeInfoParams
        dto.serde.parameters == sdParams
        dto.fields.size() == columns.size() + partitonKeys.size()
        dto.fields.each {
            it.name.startsWith('field_')
            it.comment.startsWith('comment_')
            it.type == 'VARCHAR'
        }
        dto.fields.findAll { it.partition_key }.size() == 2
        dto.metadata == tableParams

        where:
        databaseName = 'database'
        tableName = 'table'
        name = QualifiedName.ofTable('catalog', databaseName, tableName)
        owner = 'owner'
        createDate = Instant.parse('2016-02-25T14:47:27').toDate()
        location = 'location'
        inputFormat = 'inputFormat'
        outputFormat = 'outputFormat'
        serializationLib = 'serializationLib'
        serdeInfoParams = ['sipk1': 'sipv1']
        sdParams = ['sdk1': 'sdv1']
        columns = (0..7).collect { new FieldSchema(name: "field_$it", comment: "comment_$it", type: 'VARCHAR') }
        partitonKeys = (0..1).collect { new FieldSchema(name: "field_$it", comment: "comment_$it", type: 'VARCHAR') }
        tableParams = ['tk1': 'tv1']
        table = new Table(
            dbName: databaseName,
            tableName: tableName,
            owner: owner,
            createTime: Instant.parse('2016-02-25T14:47:27').getMillis() / 1000,
            sd: new StorageDescriptor(
                cols: columns,
                location: location,
                inputFormat: inputFormat,
                outputFormat: outputFormat,
                serdeInfo: new SerDeInfo(
                    name: 'serdeName',
                    serializationLib: serializationLib,
                    parameters: serdeInfoParams,
                ),
                parameters: sdParams
            ),
            partitionKeys: partitonKeys,
            parameters: tableParams,
        )
    }

    def 'test metacatToHivePartition sets all required fields to non-null values'() {
        given:
        TableDto tableDto = new TableDto()
        PartitionDto dto = new PartitionDto()

        when:
        def partition = converter.metacatToHivePartition(dto, tableDto)

        then:
        partition
        partition.values != null
        partition.tableName == null
        partition.dbName == null
        partition.parameters != null
        partition.sd != null
        partition.sd.cols != null
        partition.sd.location == null
        partition.sd.inputFormat == null
        partition.sd.outputFormat == null
        partition.sd.serdeInfo != null
        partition.sd.serdeInfo.name == null
        partition.sd.serdeInfo.serializationLib == null
        partition.sd.serdeInfo.parameters != null
        partition.sd.bucketCols != null
        partition.sd.sortCols != null
        partition.sd.parameters != null
    }

    def 'test metacatToHivePartition'() {
        when:
        def partition = converter.metacatToHivePartition(dto, tableDto)

        then:
        partition.values == ['CAPS', 'lower']
        partition.tableName == tableName
        partition.dbName == databaseName
        partition.createTime == Instant.parse('2016-02-25T14:47:27').getMillis() / 1000
        partition.lastAccessTime == Instant.parse('2016-02-25T14:47:27').getMillis() / 1000
        partition.sd.cols.size() == fields.size() - 2
        partition.sd.cols.each {
            it.name.startsWith('field_')
            it.comment.startsWith('comment_')
            it.type == 'VARCHAR'
        }
        partition.sd.location == location
        partition.sd.inputFormat == inputFormat
        partition.sd.outputFormat == outputFormat
        partition.sd.serdeInfo.name == tableName
        partition.sd.serdeInfo.serializationLib == serializationLib
        partition.sd.serdeInfo.parameters == serdeInfoParams
        partition.sd.parameters == sdParams
        partition.parameters == partitionParams

        where:
        databaseName = 'database'
        tableName = 'table'
        partitionName = 'field_0=CAPS/field_1=lower'
        owner = 'owner'
        createDate = Instant.parse('2016-02-25T14:47:27').toDate()
        location = 'location'
        inputFormat = 'inputFormat'
        outputFormat = 'outputFormat'
        serializationLib = 'serializationLib'
        sdParams = ['sdk1': 'sdv1']
        serdeInfoParams = ['sipk1': 'sipv1']
        fields = (0..9).collect {
            new FieldDto(name: "field_$it", partition_key: it < 2, comment: "comment_$it", type: 'VARCHAR')
        }
        partitionParams = ['tk1': 'tv1']
        dto = new PartitionDto(
            name: QualifiedName.ofPartition('catalog', databaseName, tableName, partitionName),
            audit: new AuditDto(createdDate: createDate, lastModifiedDate: createDate),
            serde: new StorageDto(
                owner: owner,
                uri: location,
                inputFormat: inputFormat,
                outputFormat: outputFormat,
                serializationLib: serializationLib,
                parameters: sdParams,
                serdeInfoParameters: serdeInfoParams,
            ),
            metadata: partitionParams,
        )
        tableDto = new TableDto(
            fields: fields
        )
    }

    def 'test metacatToHivePartition can handle a partition name with multiple equals'() {
        when:
        def partition = converter.metacatToHivePartition(dto, tableDto)

        then:
        partition.values == ['true', 'monk']

        where:
        dto = new PartitionDto(
            name: QualifiedName.ofPartition('c', 'd', 't', 'this=weird=true/someone=monk')
        )
        tableDto = new TableDto()
    }

    def 'test metacatToHivePartition throws no error on invalid partition name'() {
        when:
        converter.metacatToHivePartition(dto, tableDto)

        then:
        noExceptionThrown()

        where:
        dto = new PartitionDto(
            name: QualifiedName.ofPartition('c', 'd', 't', 'fail')
        )
        tableDto = new TableDto()
    }

    def 'test metacatToHivePartition copies serialization lib from the table if there is not one on the partition'() {
        when:
        def partition = converter.metacatToHivePartition(dto, tableDto)

        then:
        partition.sd.serdeInfo.serializationLib == serializationLib

        where:
        serializationLib = 'serializationLib'
        dto = new PartitionDto()
        tableDto = new TableDto(
            serde: new StorageDto(
                serializationLib: serializationLib
            )
        )
    }

    def 'test hiveToMetacatPartition'() {
        when:
        def dto = converter.hiveToMetacatPartition(tableDto, partition)

        then:
        dto.name == QualifiedName.ofPartition('catalog', databaseName, tableName, 'key1=CAPS/key2=lower/key3=3')
        dto.name.databaseName == databaseName
        dto.name.tableName == tableName
        dto.audit.createdDate == Instant.parse('2016-02-25T14:47:27').toDate()
        dto.serde.owner == owner
        dto.serde.uri == location
        dto.serde.inputFormat == inputFormat
        dto.serde.outputFormat == outputFormat
        dto.serde.serializationLib == serializationLib
        dto.serde.serdeInfoParameters == serdeParams
        dto.serde.parameters == sdParams
        dto.metadata == partitionParams

        where:
        databaseName = 'database'
        tableName = 'table'
        owner = 'owner'
        createDate = Instant.parse('2016-02-25T14:47:27').toDate()
        location = 'location'
        inputFormat = 'inputFormat'
        outputFormat = 'outputFormat'
        serializationLib = 'serializationLib'
        serdeParams = ['k1': 'v1']
        sdParams = ['sdk1': 'sdv1']
        partitionParams = ['tk1': 'tv1']
        partition = new Partition(
            dbName: databaseName,
            tableName: tableName,
            createTime: Instant.parse('2016-02-25T14:47:27').getMillis() / 1000,
            values: ['CAPS', 'lower', '3'],
            sd: new StorageDescriptor(
                cols: [new FieldSchema('col1', 'VARCHAR', 'comment_1')],
                location: location,
                inputFormat: inputFormat,
                outputFormat: outputFormat,
                serdeInfo: new SerDeInfo(
                    name: 'serdeName',
                    serializationLib: serializationLib,
                    parameters: serdeParams,
                ),
                parameters: sdParams
            ),
            parameters: partitionParams,
        )
        tableDto = new TableDto(
            name: QualifiedName.ofTable('catalog', databaseName, tableName),
            fields: [
                new FieldDto(name: 'key1', partition_key: true, comment: 'comment_1', type: 'VARCHAR'),
                new FieldDto(name: 'key2', partition_key: true, comment: 'comment_2', type: 'VARCHAR'),
                new FieldDto(name: 'key3', partition_key: true, comment: 'comment_3', type: 'VARCHAR'),
                new FieldDto(name: 'col1', partition_key: false, comment: 'comment_1', type: 'VARCHAR'),
            ],
            serde: new StorageDto(
                owner: owner
            ),
        )
    }

    def 'test hiveToMetacatPartition fails if wrong number of partition values'() {
        given:
        def partition = new Partition(
            values: partitionValues
        )
        def tableDto = new TableDto(
            name: QualifiedName.ofTable('c', 'd', 't'),
            fields: [
                new FieldDto(name: 'key1', partition_key: true, comment: 'comment_1', type: 'VARCHAR'),
                new FieldDto(name: 'key2', partition_key: true, comment: 'comment_2', type: 'VARCHAR'),
                new FieldDto(name: 'key3', partition_key: true, comment: 'comment_3', type: 'VARCHAR'),
                new FieldDto(name: 'col1', partition_key: false, comment: 'comment_1', type: 'VARCHAR'),
            ]
        )

        when:
        converter.hiveToMetacatPartition(tableDto, partition)

        then:
        thrown(IllegalArgumentException)

        where:
        partitionValues << [[], ['too few'], ['still', 'too few'], ['too', 'many', 'values', 'present']]
    }
}
