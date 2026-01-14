/*
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.metacat.common.server.converter.converters;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connector.iceberg.IcebergTableWrapper;
import com.netflix.metacat.common.server.connector.sql.DirectSqlTable;
import com.netflix.metacat.common.server.connectors.model.FieldInfo;
import com.netflix.metacat.common.server.connectors.model.StorageInfo;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.converter.IcebergTypeConverter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converter for Iceberg tables to Metacat TableInfo.
 * This class provides conversion utilities for Iceberg tables without
 * requiring Hive dependencies.
 *
 * @author metacat team
 * @since 1.4.0
 */
@Slf4j
public class IcebergTableInfoConverter {

    private final IcebergTypeConverter icebergTypeConverter;

    /**
     * Constructor.
     *
     * @param icebergTypeConverter type converter for schema conversion
     */
    public IcebergTableInfoConverter(final IcebergTypeConverter icebergTypeConverter) {
        this.icebergTypeConverter = icebergTypeConverter;
    }

    /**
     * Default constructor using default IcebergTypeConverter.
     */
    public IcebergTableInfoConverter() {
        this.icebergTypeConverter = new IcebergTypeConverter();
    }

    /**
     * Converts IcebergTable to TableInfo.
     *
     * @param name             qualified name
     * @param tableWrapper     iceberg table wrapper containing the table info and extra properties
     * @param tableLoc         iceberg table metadata location
     * @param tableInfo        table info with existing metadata (e.g., serde info)
     * @return Metacat table Info
     */
    public TableInfo fromIcebergTableToTableInfo(final QualifiedName name,
                                                 final IcebergTableWrapper tableWrapper,
                                                 final String tableLoc,
                                                 final TableInfo tableInfo) {
        final org.apache.iceberg.Table table = tableWrapper.getTable();
        final List<FieldInfo> allFields =
            this.icebergTypeConverter.icebergSchemaToFieldInfo(table.schema(), table.spec().fields());
        final Map<String, String> tableParameters = new HashMap<>();
        tableParameters.put(DirectSqlTable.PARAM_TABLE_TYPE, DirectSqlTable.ICEBERG_TABLE_TYPE);
        tableParameters.put(DirectSqlTable.PARAM_METADATA_LOCATION, tableLoc);
        tableParameters.put(DirectSqlTable.PARAM_PARTITION_SPEC, table.spec().toString());
        //adding iceberg table properties
        tableParameters.putAll(table.properties());

        // Populate branch/tag metadata for optimization purposes
        tableParameters.putAll(tableWrapper.populateBranchTagMetadata());
        tableParameters.putAll(tableWrapper.getExtraProperties());
        final StorageInfo.StorageInfoBuilder storageInfoBuilder = StorageInfo.builder();
        if (tableInfo.getSerde() != null) {
            // Adding the serde properties to support old engines.
            storageInfoBuilder.inputFormat(tableInfo.getSerde().getInputFormat())
                .outputFormat(tableInfo.getSerde().getOutputFormat())
                .uri(tableInfo.getSerde().getUri())
                .serializationLib(tableInfo.getSerde().getSerializationLib());
        }
        return TableInfo.builder().fields(allFields)
            .metadata(tableParameters)
            .serde(storageInfoBuilder.build())
            .name(name).auditInfo(tableInfo.getAudit())
            .build();
    }
}
