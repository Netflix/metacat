/*
 *  Copyright 2020 Netflix, Inc.
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

package com.netflix.metacat.connector.hive.sql;

import com.netflix.metacat.common.QualifiedName;
import com.netflix.metacat.common.server.connectors.model.TableInfo;
import com.netflix.metacat.connector.hive.commonview.CommonViewHandler;
import com.netflix.metacat.connector.hive.converters.HiveConnectorInfoConverter;
import com.netflix.metacat.connector.hive.converters.HiveTypeConverter;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableHandler;
import com.netflix.metacat.connector.hive.iceberg.IcebergTableWrapper;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;

/**
 * Proxy class to get the metadata info from cache if exists.
 */
@CacheConfig(cacheNames = "metacat")
public class HiveConnectorFastTableServiceProxy {
    private final IcebergTableHandler icebergTableHandler;
    private final HiveConnectorInfoConverter hiveMetacatConverters;
    private final CommonViewHandler commonViewHandler;

    /**
     * Constructor.
     *
     * @param hiveMetacatConverters        hive converter
     * @param icebergTableHandler          iceberg table handler
     * @param commonViewHandler            common view handler
     */
    public HiveConnectorFastTableServiceProxy(
        final HiveConnectorInfoConverter hiveMetacatConverters,
        final IcebergTableHandler icebergTableHandler,
        final CommonViewHandler commonViewHandler
    ) {
        this.hiveMetacatConverters = hiveMetacatConverters;
        this.icebergTableHandler = icebergTableHandler;
        this.commonViewHandler = commonViewHandler;
    }

    /**
     * Return the table metadata from cache if exists. If not exists, make the iceberg call to refresh it.
     * @param tableName             table name
     * @param tableMetadataLocation table metadata location
     * @param info                  table info stored in hive metastore
     * @param includeInfoDetails    if true, will include more details like the manifest file content
     * @param useCache true, if table can be retrieved from cache
     * @return TableInfo
     */
    @Cacheable(key = "'iceberg.table.' + #includeInfoDetails + '.' + #tableMetadataLocation", condition = "#useCache")
    public TableInfo getIcebergTable(final QualifiedName tableName,
                                     final String tableMetadataLocation,
                                     final TableInfo info,
                                     final boolean includeInfoDetails,
                                     final boolean useCache) {
        final IcebergTableWrapper icebergTable =
            this.icebergTableHandler.getIcebergTable(tableName, tableMetadataLocation, includeInfoDetails);
        return this.hiveMetacatConverters.fromIcebergTableToTableInfo(tableName,
            icebergTable, tableMetadataLocation, info);
    }

    /**
     * Return the common view metadata from cache if exists. If not exists, make the common view handler call
     * to refresh it.
     * @param name                  common view name
     * @param tableMetadataLocation common view metadata location
     * @param info                  common view info stored in hive metastore
     * @param hiveTypeConverter     hive type converter
     * @param useCache true, if table can be retrieved from cache
     * @return TableInfo
     */
    @Cacheable(key = "'iceberg.view.' + #tableMetadataLocation", condition = "#useCache")
    public TableInfo getCommonViewTableInfo(final QualifiedName name,
                                     final String tableMetadataLocation,
                                     final TableInfo info,
                                     final HiveTypeConverter hiveTypeConverter,
                                     final boolean useCache) {
        return commonViewHandler.getCommonViewTableInfo(name, tableMetadataLocation, info, hiveTypeConverter);
    }
}
