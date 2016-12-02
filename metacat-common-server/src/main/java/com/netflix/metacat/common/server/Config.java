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

package com.netflix.metacat.common.server;

import com.netflix.metacat.common.QualifiedName;

import java.util.List;

/**
 * Property configurations.
 */
public interface Config {
    /**
     * Default type converter.
     * @return Default type converter
     */
    String getDefaultTypeConverter();
    /**
     * Enable elastic search.
     * @return true if elastic search is enabled
     */
    boolean isElasticSearchEnabled();
    /**
     * Elastic search cluster name.
     * @return cluster name
     */
    String getElasticSearchClusterName();
    /**
     * Comm aseparated list of elastic search cluster nodes.
     * @return String
     */
    String getElasticSearchClusterNodes();
    /**
     * Elastic search cluster port.
     * @return cluster port
     */
    int getElasticSearchClusterPort();
    /**
     * Elastic search fetch size.
     * @return elastic search fetch size
     */
    int getElasticSearchScrollFetchSize();
    /**
     * Elastic search scroll timeout.
     * @return elastic search scroll timeout
     */
    int getElasticSearchScrollTimeout();
    /**
     * Names to exclude when refreshing elastic search.
     * @return Names to exclude when refreshing elastic search
     */
    List<QualifiedName> getElasticSearchRefreshExcludeQualifiedNames();
    /**
     * Catalogs to include when refreshing elastic search.
     * @return Catalogs to include when refreshing elastic search
     */
    String getElasticSearchRefreshIncludeCatalogs();
    /**
     * Databases to include when refreshing elastic search.
     * @return Databases to include when refreshing elastic search
     */
    List<QualifiedName> getElasticSearchRefreshIncludeDatabases();
    /**
     * Catalogs to include when refreshing elastic search partitions.
     * @return Catalogs to include when refreshing elastic search partitions
     */
    String getElasticSearchRefreshPartitionsIncludeCatalogs();
    /**
     * Threshold no. of databases to delete.
     * @return Threshold no. of databases to delete
     */
    int getElasticSearchThresholdUnmarkedDatabasesDelete();
    /**
     * Threshold no. of tables to delete.
     * @return Threshold no. of tables to delete
     */
    int getElasticSearchThresholdUnmarkedTablesDelete();
    /**
     * Thread count.
     * @return thread count
     */
    int getEventBusExecutorThreadCount();
    /**
     * Event bus thread count.
     * @return thread count
     */
    int getEventBusThreadCount();
    /**
     * Hive partition white list pattern.
     * @return pattern
     */
    String getHivePartitionWhitelistPattern();
    /**
     * Lookup service admin user name.
     * @return user name
     */
    String getLookupServiceUserAdmin();
    /**
     * Metacat version.
     * @return metacat version
     */
    String getMetacatVersion();
    /**
     * Config location.
     * @return config location
     */
    String getPluginConfigLocation();
    /**
     * Tag service admin username.
     * @return username
     */
    String getTagServiceUserAdmin();
    /**
     * Thrift server max worker threads.
     * @return Thrift server max worker threads
     */
    int getThriftServerMaxWorkerThreads();
    /**
     * Thrift server client timeout.
     * @return Thrift server client timeout
     */
    int getThriftServerSocketClientTimeoutInSeconds();
    /**
     * Epoch.
     * @return epoch
     */
    boolean isEpochInSeconds();
    /**
     * Do we use pig types.
     * @return Do we use pig types
     */
    boolean isUsePigTypes();
    /**
     * Max. number of threads for service.
     * @return Max. number of threads for service
     */
    int getServiceMaxNumberOfThreads();
    /**
     * List of names for which the partition listing show throw error when no filter is specified.
     * @return list of names
     */
    List<QualifiedName> getQualifiedNamesToThrowErrorWhenNoFilterOnListPartitions();
    /**
     * Elastic search index.
     * @return elastic search index name
     */
    String getEsIndex();
    /**
     * Lifetime.
     * @return lifetime
     */
    int getDataMetadataDeleteMarkerLifetimeInDays();
    /**
     * soft delete data metadata.
     * @return true if we can delete data metadata
     */
    boolean canSoftDeleteDataMetadata();
    /**
     * cascade view and metadata delete on table delete.
     * @return true if cascade
     */
    boolean canCascadeViewsMetadataOnTableDelete();
    /**
     * Max. number of in clause items in user metadata service queries.
     * @return Max. number of in clause items in user metadata service queries
     */
    int getUserMetadataMaxInClauseItems();
}
