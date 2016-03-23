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

package com.netflix.metacat.common.util

import com.netflix.metacat.common.dto.PartitionDto
import com.netflix.metacat.common.dto.TableDto
import com.netflix.metacat.common.json.MetacatJson
import com.netflix.metacat.common.json.MetacatJsonLocator

/**
 * Created by amajumdar on 5/15/15.
 */
class DataProvider {
    private static final MetacatJson metacatJson = MetacatJsonLocator.INSTANCE

    def static TableDto getTable(String sourceName, String databaseName, String tableName, String owner, String uri){
        def f = new File('../metacat-common/src/test/resources/tableTemplate.json')
        if(!f.exists()){
            f = new File('metacat-common/src/test/resources/tableTemplate.json')
        }
        if( uri == null){
            uri = String.format("file://tmp/hive/warehouse/%s.db/%s", databaseName, tableName);
        }
        def tableJson =  String.format(f.getText(), sourceName, databaseName, tableName, owner, uri)
        return metacatJson.parseJsonValue(tableJson, TableDto.class)
    }

    def static List<TableDto> getTables(String sourceName, String databaseName, String tableName, String owner, String uri, int noOfTables){
        def result = [] as List<TableDto>
        for(int i=0;i<noOfTables;i++){
            result.add(getTable( sourceName, databaseName, tableName + i, owner, uri + i))
        }
        return result
    }

    def static List<PartitionDto> getPartitions(String sourceName, String databaseName, String tableName, String name, String uri, int noOfPartitions){
        def result = [] as List<PartitionDto>
        for(int i=0;i<noOfPartitions;i++){
            result.add(getPartition( sourceName, databaseName, tableName, name + i, uri + i))
        }
        return result
    }

    def static PartitionDto getPartition(String sourceName, String databaseName, String tableName, String name, String uri){
        def f = new File('../metacat-common/src/test/resources/partitionTemplate.json')
        if(!f.exists()){
            f = new File('metacat-common/src/test/resources/partitionTemplate.json')
        }
        if( uri == null){
            uri = String.format("file://tmp/hive/warehouse/amajumdar.db/%s/%s/batchid=1361678899",tableName, name);
        }
        def partitionJson =  String.format(f.getText(), sourceName, databaseName, tableName, name, uri)
        return metacatJson.parseJsonValue(partitionJson, PartitionDto.class)
    }
}
