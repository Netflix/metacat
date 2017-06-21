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
                definitionMetadata: getDefinitionMetadata(owner)
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
                '    "name": "Rashmi Shamprasad",\n' +
                '    "team": "dea_experimentation",\n' +
                '    "group": "dea_gama",\n' +
                '    "department": "Data Engineering and Analytics",\n' +
                '    "manager_userid_list": [\n' +
                '      "nhunt",\n' +
                '      "yury",\n' +
                '      "pellwood",\n' +
                '      "ntowery",\n' +
                '      "ebressert"\n' +
                '    ]\n' +
                '  },\n' +
                '  "job": {\n' +
                '    "name": "PG.EXP.AB_EXP_NM_CUST_ALLOCS_F_SIGNUP"\n' +
                '  },\n' +
                '  "data_hygiene": {\n' +
                '    "delete_method": "by partition column",\n' +
                '    "delete_column": "allocation_date",\n' +
                '    "data_removed": 1493220909,\n' +
                '    "data_loaded": 1493241559\n' +
                '  },\n' +
                '  "s3": {},\n' +
                '  "extendedSchema": {\n' +
                '    "fields": [\n' +
                '      {\n' +
                '        "fk_dim_column": "account_id",\n' +
                '        "type": "fk",\n' +
                '        "name": "account_id",\n' +
                '        "fk_dim_table": "prodhive/dse/account_d"\n' +
                '      },\n' +
                '      {\n' +
                '        "fk_dim_column": "country_iso_code",\n' +
                '        "type": "fk",\n' +
                '        "name": "country_code",\n' +
                '        "fk_dim_table": "prodhive/dse/geo_country_d"\n' +
                '      },\n' +
                '      {\n' +
                '        "fk_dim_column": "plan_rollup_id",\n' +
                '        "type": "fk",\n' +
                '        "name": "current_plan_rollup_id",\n' +
                '        "fk_dim_table": "prodhive/dse/plan_rollup_d"\n' +
                '      },\n' +
                '      {\n' +
                '        "fk_dim_column": "device_type_id",\n' +
                '        "type": "fk",\n' +
                '        "name": "device_type_id",\n' +
                '        "fk_dim_table": "prodhive/dse/device_type_rollup_d"\n' +
                '      },\n' +
                '      {\n' +
                '        "fk_dim_column": "promo_id",\n' +
                '        "type": "fk",\n' +
                '        "name": "landing_promo_id",\n' +
                '        "fk_dim_table": "prodhive/dse/promo_d"\n' +
                '      },\n' +
                '      {\n' +
                '        "fk_dim_column": "promo_id",\n' +
                '        "type": "fk",\n' +
                '        "name": "latest_promo_id",\n' +
                '        "fk_dim_table": "prodhive/dse/promo_d"\n' +
                '      },\n' +
                '      {\n' +
                '        "fk_dim_column": "mop_id",\n' +
                '        "type": "fk",\n' +
                '        "name": "mop_id",\n' +
                '        "fk_dim_table": "prodhive/dse/account_mop_d"\n' +
                '      },\n' +
                '      {\n' +
                '        "fk_dim_column": "plan_rollup_id",\n' +
                '        "type": "fk",\n' +
                '        "name": "signup_plan_rollup_id",\n' +
                '        "fk_dim_table": "prodhive/dse/plan_rollup_d"\n' +
                '      },\n' +
                '      {\n' +
                '        "fk_dim_column": "subregion_sk",\n' +
                '        "type": "fk",\n' +
                '        "name": "subregion_sk",\n' +
                '        "fk_dim_table": "prodhive/dse/geo_subregion_d"\n' +
                '      },\n' +
                '      {\n' +
                '        "type": "metric",\n' +
                '        "name": "current_plan_usd_price"\n' +
                '      },\n' +
                '      {\n' +
                '        "type": "metric",\n' +
                '        "name": "predicted_tenure"\n' +
                '      },\n' +
                '      {\n' +
                '        "type": "metric",\n' +
                '        "name": "realized_revenue"\n' +
                '      },\n' +
                '      {\n' +
                '        "type": "metric",\n' +
                '        "name": "total_secs"\n' +
                '      },\n' +
                '      {\n' +
                '        "name": "allocation_date",\n' +
                '        "nullable": "false"\n' +
                '      },\n' +
                '      {\n' +
                '        "name": "allocation_utc_date",\n' +
                '        "nullable": "false"\n' +
                '      },\n' +
                '      {\n' +
                '        "name": "status",\n' +
                '        "nullable": "false"\n' +
                '      }\n' +
                '    ],\n' +
                '    "table_type": "fact"\n' +
                '  },\n' +
                '  "data_dependency": {\n' +
                '    "valid_thru_utc_ts_trigger": "manual",\n' +
                '    "partition_column_date_type": "region"\n' +
                '  },\n' +
                '  "table_cost": {\n' +
                '    "s3_cost": {\n' +
                '      "size_in_bytes": 424656076571,\n' +
                '      "cost": 82.1041,\n' +
                '      "last_refreshed_on": 20170423,\n' +
                '      "description": "Annualized S3 storage cost based on all files in the table prefix, including all partitions and batchids, in dollars",\n' +
                '      "cost_history": [\n' +
                '        {\n' +
                '          "cost": 134.4294,\n' +
                '          "refresh_date": 20170212\n' +
                '        },\n' +
                '        {\n' +
                '          "cost": 129.2185,\n' +
                '          "refresh_date": 20170226\n' +
                '        },\n' +
                '        {\n' +
                '          "cost": 128.039,\n' +
                '          "refresh_date": 20170305\n' +
                '        },\n' +
                '        {\n' +
                '          "cost": 122.8049,\n' +
                '          "refresh_date": 20170312\n' +
                '        },\n' +
                '        {\n' +
                '          "cost": 115.1421,\n' +
                '          "refresh_date": 20170319\n' +
                '        },\n' +
                '        {\n' +
                '          "cost": 106.547,\n' +
                '          "refresh_date": 20170326\n' +
                '        },\n' +
                '        {\n' +
                '          "cost": 96.0612,\n' +
                '          "refresh_date": 20170402\n' +
                '        },\n' +
                '        {\n' +
                '          "cost": 88.5423,\n' +
                '          "refresh_date": 20170409\n' +
                '        },\n' +
                '        {\n' +
                '          "cost": 83.5336,\n' +
                '          "refresh_date": 20170416\n' +
                '        },\n' +
                '        {\n' +
                '          "cost": 82.1041,\n' +
                '          "refresh_date": 20170423\n' +
                '        }\n' +
                '      ]\n' +
                '    }\n' +
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
}
