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

import com.google.common.collect.Maps
import com.netflix.metacat.common.QualifiedName
import com.netflix.metacat.common.dto.*
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator
import com.netflix.metacat.common.server.connectors.ConnectorContext
import com.netflix.metacat.common.server.properties.Config
import com.netflix.metacat.common.server.properties.DefaultConfigImpl
import com.netflix.metacat.common.server.properties.MetacatProperties
import com.netflix.spectator.api.NoopRegistry

/**
 * Created by amajumdar on 5/15/15.
 */
class DataDtoProvider {
    private static final MetacatJson metacatJson = new MetacatJsonLocator()
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
                    type: 'string',
                    partition_key: true
                ),
                new FieldDto(
                    comment: 'added 2st',
                    name: 'field2',
                    pos: 1,
                    type: 'string',
                    partition_key: false
                ),
                new FieldDto(
                    comment: 'added 3st - partition key',
                    name: 'field3',
                    pos: 2,
                    type: 'string',
                    partition_key: true
                ),
                new FieldDto(
                    comment: 'added 4st',
                    name: 'field4',
                    pos: 3,
                    type: 'string',
                    partition_key: false
                )
            ],
            definitionMetadata: getDefinitionMetadata(owner),
            keys: new KeySetDto(
                partition: [ new KeyDto(
                    name: "primary",
                    fields: ["field1", "field3"]
                )]
            )
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
                        serdeInfoParameters: [
                            'serialization.format': '1'
                        ],
                        uri: uri
                ),
                metadata: [
                        'bytesSize' : '1'
                ],
                definitionMetadata: getDefinitionMetadata('test'),
                dataMetadata: metacatJson.parseJsonObject('{"metrics": {}}')
        )
    }
    /**
     * Returns a tableDto with the provided information. This table contains two columns.
     */
    def static TableDto getPartTable(String sourceName, String databaseName, String owner, String uri){
        def tableName = 'part'
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
                    type: 'string',
                    partition_key: false
                ),
                new FieldDto(
                    comment: null,
                    name: 'one',
                    pos: 1,
                    type: 'string',
                    partition_key: true
                )
            ],
            definitionMetadata: getDefinitionMetadata(owner)
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
                    type: 'string',
                    partition_key: false
                ),
                new FieldDto(
                    comment: 'added 2nt',
                    name: 'one',
                    pos: 1,
                    type: 'string',
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
            definitionMetadata: getDefinitionMetadata(owner)
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
        return metacatJson.parseJsonValue(tableJson, TableDto.class)
    }

    def static getDefinitionMetadata(String owner){
        return metacatJson.parseJsonObject('{\n' +
                '  "tags": [\n' +
                '    "data-category:detailed customer",\n' +
                '    "audience:user",\n' +
                '    "lifecycle:active",\n' +
                '    "model:report",\n' +
                '    "subject-area:deprecate ab"\n' +
                '  ],\n' +
                '  "aegisthus": {},\n' +
                '  "hive": {\n' +
                '    "cleanup": false\n' +
                '  },\n' +
                '  "rds": {},\n' +
                '  "lifetime": {\n' +
                '    "createTime": 1442267129418,\n' +
                '    "days": 300,\n' +
                '    "updateTime": 1442267129418,\n' +
                '    "user": "egustavson",\n' +
                '    "partitionedBy": "allocation_utc_date"\n' +
                '  },\n' +
                '  "owner": {\n' +
                '    "userId": \"'+owner+'\",\n' +
                '    "name": "test",\n' +
                '    "team": "test",\n' +
                '    "group": "dea_gama",\n' +
                '    "department": "test",\n' +
                '    "manager_userid_list": [\n' +
                '      "a",\n' +
                '      "b",\n' +
                '      "c",\n' +
                '      "d",\n' +
                '      "e"\n' +
                '    ]\n' +
                '  },\n' +
                '  "job": {\n' +
                '    "name": "xyz"\n' +
                '  },\n' +
                '  "data_hygiene": {\n' +
                '    "delete_method": "by partition column",\n' +
                '    "delete_column": "allocation_date",\n' +
                '    "data_removed": 1493220909,\n' +
                '    "data_loaded": 1493241559\n' +
                '  },\n' +
                '  "s3": {},\n' +
                '  "data_dependency": {\n' +
                '    "valid_thru_utc_ts_trigger": "manual",\n' +
                '    "partition_column_date_type": "region"\n' +
                '  },\n' +
                '  "search_attributes": {\n' +
                '    "data_category": "detailed customer",\n' +
                '    "subject_area": "deprecate ab",\n' +
                '    "model": "report",\n' +
                '    "audience": "user",\n' +
                '    "lifecycle": "active"\n' +
                '  },\n' +
                '  "attribute_history": {\n' +
                '    "data_category": null,\n' +
                '    "subject_area": null,\n' +
                '    "model": null,\n' +
                '    "audience": null,\n' +
                '    "lifecycle": null,\n' +
                '    "fixed": true\n' +
                '  },\n' +
                '  "data_category": "detailed customer",\n' +
                '  "subject_area": "deprecate ab",\n' +
                '  "model": {\n' +
                '    "value": "report"\n' +
                '  },\n' +
                '  "audience": {\n' +
                '    "value": "user"\n' +
                '  },\n' +
                '  "lifecycle": {\n' +
                '    "value": "active"\n' +
                '  },\n' +
                '  "attribute_details": {\n' +
                '    "subject_area": {\n' +
                '      "update_by": null,\n' +
                '      "update_ts": 1492639851,\n' +
                '      "previous_value": null,\n' +
                '      "last_value": "ab deprecate"\n' +
                '    }\n' +
                '  }\n' +
                '}')
    }

    def static ConnectorContext newContext(Config conf, Map<String, String> configuration) {
        return new ConnectorContext('testhive', 'testhive', 'hive',
            conf == null ? new DefaultConfigImpl(new MetacatProperties()): conf, new NoopRegistry(),
            null,  configuration == null ? Maps.newHashMap() : configuration)
    }
}
