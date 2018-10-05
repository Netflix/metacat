
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
import com.netflix.metacat.common.server.connectors.model.*
import com.netflix.metacat.common.type.BaseType
import com.netflix.metacat.common.type.VarcharType

/**
 * com.netflix.metacat.common.server.MetacatDataInfoProvider.
 * @author zhenl
 */
class MetacatDataInfoProvider {

    private static testdatabaseInfos = [DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "test1")).build(),
                                        DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "test2")).build()]

    private static databaseInfos = [DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "test1")).build(),
                                    DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "test2")).build(),
                                    DatabaseInfo.builder().name(QualifiedName.ofDatabase("testhive", "dev1")).build()]

    private static testtableInfos = [TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testtable1")).build(),
                                     TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testtable2")).build()]

    private static tableInfos = [TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testtable1")).build(),
                                 TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "testtable2")).build(),
                                 TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "devtable2")).build(),
                                 TableInfo.builder().name(QualifiedName.ofTable("testhive", "test1", "devtable3")).build()]

    private static databaseNames = [
            QualifiedName.ofDatabase("testhive", "dev1"),
            QualifiedName.ofDatabase("testhive", "dev2"),
            QualifiedName.ofDatabase("testhive", "test1"),
            QualifiedName.ofDatabase("testhive", "test2")
    ]

    private static testdatabaseNames = [
            QualifiedName.ofDatabase("testhive", "test1"),
            QualifiedName.ofDatabase("testhive", "test2"),
    ]

    private static tableNames = [
            QualifiedName.ofTable("testhive", "test1", "devtable2"),
            QualifiedName.ofTable("testhive", "test1", "devtable3"),
            QualifiedName.ofTable("testhive", "test1", "testtable1"),
            QualifiedName.ofTable("testhive", "test1", "testtable2"),
    ]

    private static tableNameStrings = [
            "testtable1",
            "testtable2",
            "devtable2",
            "devtable3"
    ]

    private static tables = [
            "testtable1" ,
            "testtable2",
            "devtable2",
            "devtable3"
    ]
    private static fields = [
            "fielddate": FieldInfo.builder().name("coldate").type(BaseType.DATE).sourceType("date").comment("").build(),
            "fieldint" :  FieldInfo.builder().name("colint").type(BaseType.DATE).sourceType("int").comment("").build(),
            "fieldstring" :  FieldInfo.builder().name("colstring").type(VarcharType.VARCHAR).sourceType("string").comment("").build(),
            "fieldboolean" :  FieldInfo.builder().name("colboolean").type(BaseType.BOOLEAN).sourceType("boolean").comment("").build(),
            "fielddateint": FieldInfo.builder().name("dateint").type(VarcharType.VARCHAR).sourceType("string").comment("").partitionKey(true).build(),
            "fieldhour": FieldInfo.builder().name("hour").type(BaseType.INT).sourceType("string").comment("").partitionKey(true).build()
    ]
    private static tableInfoMap = [
            //nonpartitiontable
            "testtable1" : TableInfo.builder()
                    .name(QualifiedName.ofTable("testhive", "test1", "testtable1"))
                    .fields([ fields.fielddate] )
                    .serde( StorageInfo.builder().owner("test").uri("s3://test/uri")
                    .serializationLib('org.apache.hadoop.hive.ql.io.orc.OrcSerde')
                    .outputFormat('org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat')
                    .inputFormat('org.apache.hadoop.hive.ql.io.orc.OrcInputFormat')
                    .serdeInfoParameters(['serialization.format': '1'])
                    .build())
                    .metadata ( ['tp_k1': 'tp_v1'])
                    .auditInfo( AuditInfo.builder().build())
                    .build(),
            //partitiontable
            "testtable2" : TableInfo.builder()
                    .name(QualifiedName.ofTable("testhive", "test1", "testtable2"))
                    .fields([ fields.fielddateint, fields.fieldhour, fields.fieldstring] )
                    .serde( StorageInfo.builder().owner("test").uri("s3://test/uri").build())
                    .metadata ( Collections.emptyMap())
                    .auditInfo( AuditInfo.builder().build())
                    .build(),

    ]

    private static icebergTableMap = [
        "icebergtable" : TableInfo.builder()
            .name(QualifiedName.ofTable("testhive", "test1", "icebergtable"))
            .fields([ fields.fielddate] )
            .serde( StorageInfo.builder().owner("test")
            .build())
            .metadata ( ['table_type': 'ICEBERG'])
            .auditInfo( AuditInfo.builder().build())
            .build()
    ]

    private static partitionInfoMap = [
            "date=20170101/hour=1" : PartitionInfo.builder()
                    .name(QualifiedName.ofPartition("testhive", "test1", "testtable2", "date=20170101/hour=1"))
                    .auditInfo( AuditInfo.builder().build())
                    .metadata(Collections.emptyMap())
                    .build()
    ]

    def static List<DatabaseInfo> getAllTestDatabaseInfo(){
        return testdatabaseInfos;
    }

    def static List<QualifiedName> getAllDatabaseNames(){
        return databaseNames;
    }

    def static List<QualifiedName> getAllTestDatabaseName(){
        return testdatabaseNames;
    }

    def static List<QualifiedName> getAllTableNames(){
        return tableNames;
    }
    def static List<String> getTables(){
        return tableNameStrings;
    }

    def static TableInfo getTableInfo(String tableName) {
        return tableInfoMap.get(tableName)
    }

    def static TableInfo getIcebergTableInfo(String tableName) {
        return icebergTableMap.get(tableName)
    }
}
