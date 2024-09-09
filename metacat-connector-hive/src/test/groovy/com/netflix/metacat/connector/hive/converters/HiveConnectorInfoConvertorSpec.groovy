/*
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
 */

package com.netflix.metacat.connector.hive.converters

import org.apache.iceberg.PartitionField
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.transforms.Identity
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.connectors.model.AuditInfo
import com.netflix.metacat.common.server.connectors.model.DatabaseInfo
import com.netflix.metacat.common.server.connectors.model.FieldInfo
import com.netflix.metacat.common.server.connectors.model.PartitionInfo
import com.netflix.metacat.common.server.connectors.model.StorageInfo
import com.netflix.metacat.common.server.connectors.model.TableInfo
import com.netflix.metacat.common.type.VarcharType
import com.netflix.metacat.connector.hive.iceberg.IcebergTableWrapper
import com.netflix.metacat.connector.hive.sql.DirectSqlTable
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.Partition
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table
import org.joda.time.Instant
import spock.lang.Specification

import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset

/**
 * Unit test for hive connector info convertor.
 *
 * @author zhenl
 * @since 1.0.0HiveConnectorFastPartitionSpec
 */
class HiveConnectorInfoConvertorSpec extends Specification{
    private static final ZoneOffset PACIFIC = LocalDateTime.now().atZone(ZoneId.of('America/Los_Angeles')).offset
    Config config = Mock(Config)
    HiveConnectorInfoConverter converter

    def setup() {
        // Stub this to always return true
        config.isEpochInSeconds() >> true
        config.omitVoidTransformEnabled() >> true
        converter = new HiveConnectorInfoConverter(new HiveTypeConverter(config))
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
        thrown(ArithmeticException)

        where:
        input = Date.from(LocalDateTime.of(9999, 2, 25, 14, 47, 27).toInstant(PACIFIC))
    }

    def 'test fromDatabaseInfo sets all required fields to non-null values'() {
        given:
        def dbInfo = new DatabaseInfo()

        when:
        def db = converter.fromDatabaseInfo(dbInfo)

        then:
        db
        db.name != null
        db.description != null
        db.locationUri == null
        db.parameters != null
    }

    def 'test fromDatabaseInfo'() {
        given:
        def dbInfo = new DatabaseInfo(
            name: QualifiedName.ofDatabase('catalog', databaseName),
            uri: dbUri,
            metadata: metadata
        )

        when:
        def db = converter.fromDatabaseInfo(dbInfo)

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

    def 'test fromTableInfo sets all required fields to non-null values'() {
        given:
        TableInfo tableInfo = new TableInfo()

        when:
        def table = converter.fromTableInfo(tableInfo)

        then:
        table
        table.tableName != null
        table.dbName != null
        table.owner != null
        table.sd != null
        table.sd.cols != null
        table.sd.location != null
        table.sd.inputFormat == null
        table.sd.outputFormat == null
        table.sd.serdeInfo != null
        table.sd.serdeInfo.name != null
        table.sd.serdeInfo.serializationLib == null
        table.sd.serdeInfo.parameters != null
        table.sd.bucketCols != null
        table.sd.sortCols != null
        table.sd.parameters != null
        table.partitionKeys != null
        table.parameters != null
        table.tableType != null
    }

    def 'test fromTableInfo'() {
        when:
        def table = converter.fromTableInfo(tableInfo)

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
        table.sd.serdeInfo.name == ''
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
            new FieldInfo(name: "field_$it", partitionKey: it < 2, comment: "comment_$it", type: VarcharType.VARCHAR)
        }
        tableParams = ['tk1': 'tv1']
        tableInfo = new TableInfo(
            name: QualifiedName.ofTable('catalog', databaseName, tableName),
            audit: new AuditInfo(createdDate: createDate),
            serde: new StorageInfo(
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

    def 'test toTableInfo'() {
        when:
        def tableInfo = converter.toTableInfo(name, table)

        then:
        tableInfo.name == name
        tableInfo.name.databaseName == databaseName
        tableInfo.name.tableName == tableName
        tableInfo.audit.createdDate == Instant.parse('2016-02-25T14:47:27').toDate()
        tableInfo.serde.owner == owner
        tableInfo.serde.uri == location
        tableInfo.serde.inputFormat == inputFormat
        tableInfo.serde.outputFormat == outputFormat
        tableInfo.serde.serializationLib == serializationLib
        tableInfo.serde.serdeInfoParameters == serdeInfoParams
        tableInfo.serde.parameters == sdParams
        tableInfo.fields.size() == columns.size() + partitonKeys.size()
        tableInfo.fields.each {
            it.name.startsWith('field_')
            it.comment.startsWith('comment_')
            it.type == VarcharType.VARCHAR
        }
        tableInfo.fields.findAll { it.partitionKey }.size() == 2
        tableInfo.metadata == tableParams

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

    def 'test fromPartitionInfo sets all required fields to non-null values'() {
        given:
        TableInfo tableInfo = new TableInfo()
        PartitionInfo partitionInfo = new PartitionInfo()

        when:
        def partition = converter.fromPartitionInfo(tableInfo, partitionInfo)

        then:
        partition
        partition.values != null
        partition.tableName != null
        partition.dbName != null
        partition.parameters != null
        partition.sd != null
        partition.sd.cols != null
        partition.sd.location != null
        partition.sd.inputFormat == null
        partition.sd.outputFormat == null
        partition.sd.serdeInfo != null
        partition.sd.serdeInfo.name != null
        partition.sd.serdeInfo.serializationLib == null
        partition.sd.serdeInfo.parameters != null
        partition.sd.bucketCols != null
        partition.sd.sortCols != null
        partition.sd.parameters != null
    }

    def 'test metacatToHivePartition'() {
        when:
        def partition = converter.fromPartitionInfo(tableInfo,partitionInfo)

        then:
        partition.values == ['CAPS', 'lower', '3']
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
        partition.sd.serdeInfo.name == ''
        partition.sd.serdeInfo.serializationLib == serializationLib
        partition.sd.serdeInfo.parameters == serdeInfoParams
        partition.sd.parameters == sdParams
        partition.parameters == partitionParams

        where:
        databaseName = 'database'
        tableName = 'table'
        partitionName = 'key1=CAPS/key2=lower/key3=3'
        owner = 'owner'
        createDate = Instant.parse('2016-02-25T14:47:27').toDate()
        location = 'location'
        inputFormat = 'inputFormat'
        outputFormat = 'outputFormat'
        serializationLib = 'serializationLib'
        sdParams = ['sdk1': 'sdv1']
        serdeInfoParams = ['sipk1': 'sipv1']
        fields = (0..9).collect {
            new FieldInfo(name: "field_$it", partitionKey: it < 2, comment: "comment_$it", type: VarcharType.VARCHAR)
        }
        partitionParams = ['tk1': 'tv1']
        partitionInfo = new PartitionInfo(
            name: QualifiedName.ofPartition('catalog', databaseName, tableName, partitionName),
            audit: new AuditInfo(createdDate: createDate, lastModifiedDate: createDate),
            serde: new StorageInfo(
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
        tableInfo = new TableInfo(
            fields: fields
        )
    }

    def 'test fromPartitionInfo can handle a partition name with multiple equals'() {
        when:
        def partition = converter.fromPartitionInfo(tableInfo,partitionInfo)

        then:
        partition.values == ['weird=true', '', 'monk']

        where:
        partitionInfo = new PartitionInfo(
            name: QualifiedName.ofPartition('c', 'd', 't', 'this=weird=true/bob=/someone=monk')
        )
        tableInfo = new TableInfo()
    }

    def 'test fromPartitionInfo throws an error on invalid partition name'() {
        when:
        converter.fromPartitionInfo(tableInfo,partitionInfo)

        then:
        thrown(IllegalStateException)

        where:
        partitionInfo = new PartitionInfo(
            name: QualifiedName.ofPartition('c', 'd', 't', 'fail')
        )
        tableInfo = new TableInfo()
    }

    def 'test fromPartitionInfo copies serialization lib from the table if there is not one on the partition'() {
        when:
        def partition = converter.fromPartitionInfo(tableInfo,partitionInfo)

        then:
        partition.sd.serdeInfo.serializationLib == serializationLib

        where:
        serializationLib = 'serializationLib'
        partitionInfo = new PartitionInfo()
        tableInfo = new TableInfo(
            serde: new StorageInfo(
                serializationLib: serializationLib
            )
        )
    }

    def 'test toPartitionInfo'() {
        when:
        def partitionInfo = converter.toPartitionInfo(tableInfo,partition)

        then:
        partitionInfo.name == QualifiedName.ofPartition('catalog', databaseName, tableName, 'key1=CAPS/key2=lower/key3=3')
        partitionInfo.name.databaseName == databaseName
        partitionInfo.name.tableName == tableName
        partitionInfo.audit.createdDate == Instant.parse('2016-02-25T14:47:27').toDate()
        partitionInfo.serde.owner == owner
        partitionInfo.serde.uri == location
        partitionInfo.serde.inputFormat == inputFormat
        partitionInfo.serde.outputFormat == outputFormat
        partitionInfo.serde.serializationLib == serializationLib
        partitionInfo.serde.serdeInfoParameters == serdeParams
        partitionInfo.serde.parameters == sdParams
        partitionInfo.metadata == partitionParams

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
        tableInfo = new TableInfo(
            name: QualifiedName.ofTable('catalog', databaseName, tableName),
            fields: [
                new FieldInfo(name: 'key1', partitionKey: true, comment: 'comment_1', type: VarcharType.VARCHAR),
                new FieldInfo(name: 'key2', partitionKey: true, comment: 'comment_2', type: VarcharType.VARCHAR),
                new FieldInfo(name: 'key3', partitionKey: true, comment: 'comment_3', type: VarcharType.VARCHAR),
                new FieldInfo(name: 'col1', partitionKey: false, comment: 'comment_1', type: VarcharType.VARCHAR),
            ],
            serde: new StorageInfo(
                owner: owner
            ),
        )
    }

    def 'test toPartitionInfo fails if wrong number of partition values'() {
        given:
        def partition = new Partition(
            values: partitionValues
        )
        def tableInfo = new TableInfo(
            name: QualifiedName.ofTable('c', 'd', 't'),
            fields: [
                new FieldInfo(name: 'key1', partitionKey: true, comment: 'comment_1', type: VarcharType.VARCHAR),
                new FieldInfo(name: 'key2', partitionKey: true, comment: 'comment_2', type: VarcharType.VARCHAR),
                new FieldInfo(name: 'key3', partitionKey: true, comment: 'comment_3', type: VarcharType.VARCHAR),
                new FieldInfo(name: 'col1', partitionKey: false, comment: 'comment_1', type: VarcharType.VARCHAR),
            ]
        )

        when:
        converter.toPartitionInfo(tableInfo, partition)

        then:
        thrown(IllegalArgumentException)

        where:
        partitionValues << [[], ['too few'], ['still', 'too few'], ['too', 'many', 'values', 'present']]
    }

    def "test fromIcebergTableToTableInfo"() {
        def icebergTable = Mock(org.apache.iceberg.Table)
        def icebergTableWrapper = new IcebergTableWrapper(icebergTable, [:])
        def partSpec = Mock(PartitionSpec)
        def field = Mock(PartitionField)
        def schema =  Mock(Schema)
        def nestedField = Mock(Types.NestedField)
        def nestedField2 = Mock(Types.NestedField)
        def type = Mock(Type)
        when:
        def tableInfo = converter.fromIcebergTableToTableInfo(QualifiedName.ofTable('c', 'd', 't'),
            icebergTableWrapper, "/tmp/test", TableInfo.builder().build() )
        then:
        2 * field.transform() >> Mock(Identity)
        1 * icebergTable.properties() >> ["test":"abd"]
        2 * icebergTable.spec() >>  partSpec
        1 * partSpec.fields() >> [ field]
        1 * icebergTable.schema() >> schema
        1 * schema.columns() >> [nestedField, nestedField2]
        3 * nestedField.name() >> "fieldName"
        1 * nestedField.doc() >> "fieldName doc"
        1 * nestedField.type() >> type
        1 * nestedField.isOptional() >> true
        2 * nestedField2.name() >> "fieldName2"
        1 * nestedField2.doc() >> "fieldName2 doc"
        1 * nestedField2.type() >> type
        1 * nestedField2.isOptional() >> true
        2 * type.typeId() >> Type.TypeID.BOOLEAN
        1 * schema.findField(_) >> nestedField

        tableInfo.getMetadata().get(DirectSqlTable.PARAM_TABLE_TYPE).equals(DirectSqlTable.ICEBERG_TABLE_TYPE)
        tableInfo.getMetadata().get(DirectSqlTable.PARAM_METADATA_LOCATION).equals("/tmp/test")
        tableInfo.getFields().size() == 2
        tableInfo.getFields().get(0).isPartitionKey() != tableInfo.getFields().get(1).isPartitionKey()
        tableInfo.getFields().get(0).getComment() == 'fieldName doc'

    }
}
