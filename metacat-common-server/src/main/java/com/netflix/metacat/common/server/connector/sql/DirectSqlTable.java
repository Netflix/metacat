/*
 *
 *  Copyright 2018 Netflix, Inc.
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
 *
 */
package com.netflix.metacat.common.server.connector.sql;

import com.google.common.collect.ImmutableSet;
import com.netflix.metacat.common.server.connectors.model.TableInfo;

import java.util.Set;

/**
 * Interface for direct SQL table operations and constants.
 *
 * @author amajumdar
 * @since 1.2.0
 */
public interface DirectSqlTable {
    /**
     * DateCreated field users can request to sort on.
     */
    String FIELD_DATE_CREATED = "dateCreated";
    /**
     * Defines the table type.
     */
    String PARAM_TABLE_TYPE = "table_type";
    /**
     * Defines the current metadata location of the iceberg table.
     */
    String PARAM_METADATA_LOCATION = "metadata_location";
    /**
     * Defines the previous metadata location of the iceberg table.
     */
    String PARAM_PREVIOUS_METADATA_LOCATION = "previous_metadata_location";
    /**
     * Defines the current partition spec expression of the iceberg table.
     */
    String PARAM_PARTITION_SPEC = "partition_spec";
    /**
     * Iceberg table type.
     */
    String ICEBERG_TABLE_TYPE = "ICEBERG";

    /**
     * VIRTUAL_VIEW table type.
     */
    String VIRTUAL_VIEW_TABLE_TYPE = "VIRTUAL_VIEW";
    /**
     * Defines the metadata content of the iceberg table.
     */
    String PARAM_METADATA_CONTENT = "metadata_content";
    /**
     * List of parameter that needs to be excluded when updating an iceberg table.
     */
    Set<String> TABLE_EXCLUDED_PARAMS =
        ImmutableSet.of(PARAM_PARTITION_SPEC, PARAM_METADATA_CONTENT);

    /**
     * Locks and updates the iceberg table for update so that no other request can modify the table at the same time.
     * @param tableInfo table info
     */
    void updateIcebergTable(TableInfo tableInfo);
}
