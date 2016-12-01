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

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.type.TypeManager;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.dto.PartitionDto;
import com.netflix.metacat.common.dto.TableDto;

/**
 * Converters.
 */
public interface PrestoConverters {
    /**
     * Converts from table DTO to presto Table.
     * @param name name
     * @param table table info
     * @param typeManager manager
     * @return TableMetadata
     */
    TableMetadata fromTableDto(QualifiedName name, TableDto table, TypeManager typeManager);

    /**
     * Converts qualified name to presto table name.
     * @param name qualified name
     * @return presto table name
     */
    QualifiedTableName getQualifiedTableName(QualifiedName name);

    /**
     * Converts from presto partition object to Partition DTO.
     *
     * @param name name
     * @param partition partition info
     * @return Partition DTO
     */
    PartitionDto toPartitionDto(QualifiedName name, ConnectorPartition partition);

    /**
     * Converts presto table name to qualified name.
     * @param qualifiedTableName table name
     * @return qualified name
     */
    QualifiedName toQualifiedName(QualifiedTableName qualifiedTableName);

    /**
     * Converts from presto table to Table DTO.
     * @param name name
     * @param type type
     * @param ptm table info
     * @return TableDto
     */
    TableDto toTableDto(QualifiedName name, String type, TableMetadata ptm);

    /**
     * Converts from partition DTO to presto partition.
     * @param partitionDto partition info
     * @return ConnectorPartition
     */
    ConnectorPartition fromPartitionDto(PartitionDto partitionDto);
}
