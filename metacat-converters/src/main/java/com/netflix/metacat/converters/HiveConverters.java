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

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.DatabaseDto;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

/**
 * Hive converter interface.
 */
public interface HiveConverters {
    /**
     * Converts from hive table to metacat table info.
     * @param name name
     * @param table table
     * @return table info
     */
    TableDto hiveToMetacatTable(QualifiedName name, Table table);

    /**
     * Converts from metacat database info to hive database info.
     * @param databaseDto database
     * @return database
     */
    Database metacatToHiveDatabase(DatabaseDto databaseDto);

    /**
     * Converts from metacat table info to hive table info.
     * @param dto table
     * @return table
     */
    Table metacatToHiveTable(TableDto dto);

    /**
     * Converts from hive partition info to metacat partition info.
     * @param tableDto table
     * @param partition partition
     * @return partition info
     */
    PartitionDto hiveToMetacatPartition(TableDto tableDto, Partition partition);

    /**
     * Gets the partition values from the partition name.
     * @param tableDto table
     * @param partName partition name
     * @return partition info
     */
    List<String> getPartValsFromName(TableDto tableDto, String partName);

    /**
     * Gets the partition name from partition values.
     * @param tableDto table
     * @param partVals partition values
     * @return partition name
     */
    String getNameFromPartVals(TableDto tableDto, List<String> partVals);

    /**
     * Converts from metacat partition info to hive partition info.
     * @param tableDto table
     * @param partitionDto partition
     * @return partition info
     */
    Partition metacatToHivePartition(PartitionDto partitionDto, TableDto tableDto);
}
