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

package com.netflix.metacat.converters;

import com.facebook.presto.spi.type.TypeManager;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

public interface HiveConverters {
    TableDto hiveToMetacatTable(QualifiedName name, Table table);

    Database metacatToHiveDatabase(DatabaseDto databaseDto);

    Table metacatToHiveTable(TableDto dto);

    PartitionDto hiveToMetacatPartition(TableDto tableDto, Partition partition);

    List<String> getPartValsFromName(TableDto tableDto, String partName);

    String getNameFromPartVals(TableDto tableDto, List<String> partVals);

    Partition metacatToHivePartition(PartitionDto partitionDto, TableDto tableDto);
}
