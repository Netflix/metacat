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
package com.netflix.metacat.common.server.util.hive;

import com.netflix.metacat.common.MetacatRequestContext;
import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.exception.InvalidMetaException;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.common.server.util.MetacatContextManager;
import com.netflix.metacat.common.server.util.MetacatUtils;
import com.netflix.metacat.common.server.connector.sql.DirectSqlTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;

/**
 * HiveTableUtil.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Slf4j
public final class HiveTableUtil {

    private HiveTableUtil() {
    }

    /**
     * check if the table is an Iceberg Table.
     *
     * @param tableInfo table info
     * @return true for iceberg table
     */
    public static boolean isIcebergTable(final TableInfo tableInfo) {
        final String tableTypeVal = getTableType(tableInfo);
        return DirectSqlTable.ICEBERG_TABLE_TYPE.equalsIgnoreCase(tableTypeVal);
    }

    private static String getTableType(final TableInfo tableInfo) {
        final QualifiedName tableName = tableInfo.getName();
        final String fallbackTableType = "unknown";
        final MetacatRequestContext context = MetacatContextManager.getContext();
        final Map<String, String> metadata = tableInfo.getMetadata();

        if (metadata == null) {
            context.updateTableTypeMap(tableName, fallbackTableType);
            return null;
        }
        String tableType = metadata.get(DirectSqlTable.PARAM_TABLE_TYPE);
        if (StringUtils.isBlank(tableType)) {
            tableType = fallbackTableType;
        }
        context.updateTableTypeMap(tableName, tableType);
        return tableType;
    }

    /**
     * get iceberg table metadata location.
     *
     * @param tableInfo table info
     * @return iceberg table metadata location
     */
    public static String getIcebergTableMetadataLocation(final TableInfo tableInfo) {
        return tableInfo.getMetadata().get(DirectSqlTable.PARAM_METADATA_LOCATION);
    }

    /**
     * Convert qualified name to table identifier.
     *
     * @param name qualified name
     * @return table identifier
     */
    public static TableIdentifier qualifiedNameToTableIdentifier(final QualifiedName name) {
        return TableIdentifier.parse(name.toString().replace('/', '.'));
    }

    /** check if the table is a common view.
     *
     * @param tableInfo table info
     * @return true for common view
     */
    public static boolean isCommonView(final TableInfo tableInfo) {
        return tableInfo != null && tableInfo.getMetadata() != null
                && MetacatUtils.isCommonView(tableInfo.getMetadata());
    }

    /**
     * get common view metadata location.
     *
     * @param tableInfo table info
     * @return common view metadata location
     */
    public static String getCommonViewMetadataLocation(final TableInfo tableInfo) {
        return tableInfo.getMetadata().get(DirectSqlTable.PARAM_METADATA_LOCATION);
    }

    /**
     * Throws an invalid meta exception
     * if the metadata for a table is null or empty.
     *
     * @param tableName the table name.
     * @param metadata the table metadata.
     */
    public static void throwIfTableMetadataNullOrEmpty(final QualifiedName tableName,
                                                       final Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            final String message = String.format("No parameters defined for iceberg table %s", tableName);
            log.warn(message);
            throw new InvalidMetaException(tableName, message, null);
        }
    }
}
