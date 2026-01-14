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
package com.netflix.metacat.testdata.provider

import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator

/**
 * Created by amajumdar on 5/15/15.
 */
class PigDataDtoProvider {
    private static final MetacatJson metacatJson = new MetacatJsonLocator()

    // Iceberg metadata file locations for Polaris connector tests
    private static final String PIG_TABLE_METADATA = 'file:/tmp/data/metadata/pig-table.metadata.json'
    private static final String PIG_PART_TABLE_METADATA = 'file:/tmp/data/metadata/pig-part-table.metadata.json'
    private static final String PIG_PARTS_TABLE_METADATA = 'file:/tmp/data/metadata/pig-parts-table.metadata.json'
    private static final String METACAT_ALL_TYPES_METADATA = 'file:/tmp/data/metadata/metacat-all-types.metadata.json'

    /**
     * Returns a tableDto with the provided information.
     */
    def static TableDto getTable(String sourceName, String databaseName, String tableName, String owner, String uri){
        if( uri == null){
            uri = String.format("s3://wh/%s.db/%s", databaseName, tableName);
        }
        return new TableDto(
                name: QualifiedName.ofTable(sourceName, databaseName, tableName),
                serde: new StorageDto(
                        owner: owner,
                        inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                        outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        serdeInfoParameters: [
                                'serialization.format': '1'
                        ],
                        uri: uri
                ),
                audit: new AuditDto(
                        createdBy: owner,
                        createdDate: new Date(),
                        lastModifiedBy: owner,
                        lastModifiedDate: new Date()
                ),
                fields: [
                        new FieldDto(
                                comment: 'added 1st - partition key',
                                name: 'field1',
                                pos: 0,
                                type: 'chararray',
                                partition_key: true
                        ),
                        new FieldDto(
                                comment: 'added 2st',
                                name: 'field2',
                                pos: 1,
                                type: 'chararray',
                                partition_key: false
                        ),
                        new FieldDto(
                                comment: 'added 3st - partition key',
                                name: 'field3',
                                pos: 2,
                                type: 'chararray',
                                partition_key: true
                        ),
                        new FieldDto(
                                comment: 'added 4st',
                                name: 'field4',
                                pos: 3,
                                type: '{(version: chararray,ts: long)}',
                                partition_key: false
                        )
                ],
                definitionMetadata: getDefinitionMetadata(owner),
                metadata: [
                        'table_type': 'ICEBERG',
                        'metadata_location': PIG_TABLE_METADATA
                ]
        )
    }

    /**
     * Returns a list of tableDtos with the provided information. Table names and uris will be appended by the index.
     */
    def static List<TableDto> getTables(String sourceName, String databaseName, String tableName, String owner, String uri, int noOfTables){
        def result = [] as List<TableDto>
        for(int i=0;i<noOfTables;i++){
            result.add(getTable( sourceName, databaseName, tableName + i, owner, uri + i))
        }
        return result
    }
    /**
     * Returns a list of partition dtos with the provided information. Partition names and uris will be appended by the
     * index.
     */
    def static List<PartitionDto> getPartitions(String sourceName, String databaseName, String tableName, String name, String uri, int noOfPartitions){
        def result = [] as List<PartitionDto>
        for(int i=0;i<noOfPartitions;i++){
            result.add(getPartition( sourceName, databaseName, tableName, name + i, uri + i))
        }
        return result
    }
    /**
     * Returns a Partition dto with the provided information.
     */
    def static PartitionDto getPartition(String sourceName, String databaseName, String tableName, String name, String uri){
        if( uri == null){
            uri = String.format("s3://wh/%s.db/%s/%s/batchid=1361678899", databaseName,tableName, name);
        }
        return new PartitionDto(
                name: QualifiedName.ofPartition(sourceName, databaseName, tableName, name),
                dataExternal: true,
                audit: new AuditDto(
                        createdDate: new Date(),
                        lastModifiedDate: new Date()
                ),
                serde: new StorageDto(
                        inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                        outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        uri: uri
                ),
                metadata: ['functionalTest': 'true'],
                definitionMetadata: getDefinitionMetadata('test'),
                dataMetadata: metacatJson.parseJsonObject('{"metrics": {}}')
        )
    }
    /**
     * Returns a tableDto with the provided information. This table contains two columns.
     */
    def static TableDto getPartTable(String sourceName, String databaseName, String tableName, String owner, String uri){
        if (uri == null){
            uri = String.format("s3://wh/%s.db/%s", databaseName, tableName)
        }
        return new TableDto(
                name: QualifiedName.ofTable(sourceName, databaseName, tableName),
                serde: new StorageDto(
                        owner: owner,
                        inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                        outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        serdeInfoParameters: [
                                'serialization.format': '1'
                        ],
                        uri: uri
                ),
                audit: new AuditDto(
                        createdBy: owner,
                        createdDate: new Date(),
                        lastModifiedBy: owner,
                        lastModifiedDate: new Date()
                ),
                fields: [
                        new FieldDto(
                                comment: 'testing',
                                name: 'two',
                                pos: 0,
                                type: 'chararray',
                                partition_key: false
                        ),
                        new FieldDto(
                                comment: null,
                                name: 'one',
                                pos: 1,
                                type: 'chararray',
                                partition_key: true
                        )
                ],
                definitionMetadata: getDefinitionMetadata(owner),
                metadata: [
                        'table_type': 'ICEBERG',
                        'metadata_location': PIG_PART_TABLE_METADATA
                ]
        )
    }
    /**
     * Returns a tableDto with the provided information. This table contains two columns.
     */
    def static TableDto getPartHyphenTable(String sourceName, String databaseName, String tableName, String owner, String uri){
        if (uri == null){
            uri = String.format("s3://wh/%s.db/%s", databaseName, tableName)
        }
        return new TableDto(
            name: QualifiedName.ofTable(sourceName, databaseName, tableName),
            serde: new StorageDto(
                owner: owner,
                inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                serdeInfoParameters: [
                    'serialization.format': '1'
                ],
                uri: uri
            ),
            audit: new AuditDto(
                createdBy: owner,
                createdDate: new Date(),
                lastModifiedBy: owner,
                lastModifiedDate: new Date()
            ),
            fields: [
                new FieldDto(
                    comment: 'testing',
                    name: 'two',
                    pos: 0,
                    type: 'chararray',
                    partition_key: false
                ),
                new FieldDto(
                    comment: null,
                    name: 'one-one',
                    pos: 1,
                    type: 'chararray',
                    partition_key: true
                ),
                new FieldDto(
                    comment: 'added 3nt - partition key',
                    name: 'total',
                    pos: 2,
                    type: 'int',
                    partition_key: true
                )
            ],
            definitionMetadata: getDefinitionMetadata(owner),
            metadata: [
                'table_type': 'ICEBERG',
                'metadata_location': PIG_PARTS_TABLE_METADATA
            ]
        )
    }

    /**
     * Returns a tableDto with the provided information. This table contains three columns.
     */
    def static TableDto getPartsTable(String sourceName, String databaseName, String owner, String uri){
        def tableName = 'parts'
        if (uri == null){
            uri = String.format("s3://wh/%s.db/%s", databaseName, tableName)
        }
        return new TableDto(
                name: QualifiedName.ofTable(sourceName, databaseName, tableName),
                serde: new StorageDto(
                        owner: owner,
                        inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
                        outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                        serializationLib: 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                        serdeInfoParameters: [
                                'serialization.format': '1'
                        ],
                        uri: uri
                ),
                audit: new AuditDto(
                        createdBy: owner,
                        createdDate: new Date(),
                        lastModifiedBy: owner,
                        lastModifiedDate: new Date()
                ),
                fields: [
                        new FieldDto(
                                comment: 'testing',
                                name: 'two',
                                pos: 0,
                                type: 'chararray',
                                partition_key: false
                        ),
                        new FieldDto(
                                comment: 'added 2nt',
                                name: 'one',
                                pos: 1,
                                type: 'chararray',
                                partition_key: true
                        ),
                        new FieldDto(
                                comment: 'added 3nt - partition key',
                                name: 'total',
                                pos: 2,
                                type: 'int',
                                partition_key: true
                        )
                ],
                definitionMetadata: getDefinitionMetadata(owner),
                metadata: [
                        'table_type': 'ICEBERG',
                        'metadata_location': PIG_PARTS_TABLE_METADATA
                ]
        )
    }


    /**
     * Returns a tableDto with the provided information. This table contains columns with different data types.
     */
    def static TableDto getMetacatAllTypesTable(String sourceName, String databaseName, String owner, String uri){
        def f = this.getClassLoader().getResource('metacatAllTypes.json')
        if( uri == null){
            uri = String.format("s3://wh/%s.db/%s", databaseName, 'metacat_all_types');
        }
        def tableJson =  String.format(f.getText(), sourceName, databaseName, 'metacat_all_types', owner, uri)
        def table = metacatJson.parseJsonValue(tableJson, TableDto.class)
        // Add Iceberg metadata for Polaris connector
        if (table.getMetadata() == null) {
            table.setMetadata([:])
        }
        table.getMetadata().put('table_type', 'ICEBERG')
        table.getMetadata().put('metadata_location', METACAT_ALL_TYPES_METADATA)
        return table
    }

    def static getDefinitionMetadata(String owner){
        def s = new StringBuilder('{"hive": {"cleanup": true},"tags":["unused"],"owner": {"team": "Cloud Platform Engineering","userId":')
        if (owner == null) {
            s.append('null')
        } else {
            s.append('"').append(owner).append('"')
        }
        s.append(',"name":"' + owner + '"}}')
        return metacatJson.parseJsonObject(s.toString())
    }
}
