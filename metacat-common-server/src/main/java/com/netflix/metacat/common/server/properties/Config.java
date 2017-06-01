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

package com.netflix.metacat.common.server.properties;

import com.netflix.metacat.common.QualifiedName;

import java.util.List;

/**
 * Property configurations.
 */
public interface Config {
    /**
     * Default type converter.
     *
     * @return Default type converter
     */
    String getDefaultTypeConverter();

    /**
     * Enable elastic search.
     *
     * @return true if elastic search is enabled
     */
    boolean isElasticSearchEnabled();

    /**
     * Elastic search cluster name.
     *
     * @return cluster name
     */
    String getElasticSearchClusterName();

    /**
     * Comm aseparated list of elastic search cluster nodes.
     *
     * @return String
     */
    String getElasticSearchClusterNodes();

    /**
     * Elastic search cluster port.
     *
     * @return cluster port
     */
    int getElasticSearchClusterPort();

    /**
     * Elastic search fetch size.
     *
     * @return elastic search fetch size
     */
    int getElasticSearchScrollFetchSize();

    /**
     * Elastic search scroll timeout.
     *
     * @return elastic search scroll timeout
     */
    int getElasticSearchScrollTimeout();

    /**
     * Names to exclude when refreshing elastic search.
     *
     * @return Names to exclude when refreshing elastic search
     */
    List<QualifiedName> getElasticSearchRefreshExcludeQualifiedNames();

    /**
     * Catalogs to include when refreshing elastic search.
     *
     * @return Catalogs to include when refreshing elastic search
     */
    String getElasticSearchRefreshIncludeCatalogs();

    /**
     * Databases to include when refreshing elastic search.
     *
     * @return Databases to include when refreshing elastic search
     */
    List<QualifiedName> getElasticSearchRefreshIncludeDatabases();

    /**
     * Catalogs to include when refreshing elastic search partitions.
     *
     * @return Catalogs to include when refreshing elastic search partitions
     */
    String getElasticSearchRefreshPartitionsIncludeCatalogs();

    /**
     * Threshold no. of databases to delete.
     *
     * @return Threshold no. of databases to delete
     */
    int getElasticSearchThresholdUnmarkedDatabasesDelete();

    /**
     * Threshold no. of tables to delete.
     *
     * @return Threshold no. of tables to delete
     */
    int getElasticSearchThresholdUnmarkedTablesDelete();

    /**
     * Thread count.
     *
     * @return thread count
     */
    int getEventBusExecutorThreadCount();

    /**
     * Event bus thread count.
     *
     * @return thread count
     */
    int getEventBusThreadCount();

    /**
     * Hive partition white list pattern.
     *
     * @return pattern
     */
    String getHivePartitionWhitelistPattern();

    /**
     * Lookup service admin user name.
     *
     * @return user name
     */
    String getLookupServiceUserAdmin();

    /**
     * Metacat version.
     *
     * @return metacat version
     */
    String getMetacatVersion();

    /**
     * Config location.
     *
     * @return config location
     */
    String getPluginConfigLocation();

    /**
     * Tag service admin username.
     *
     * @return username
     */
    String getTagServiceUserAdmin();

    /**
     * Thrift server max worker threads.
     *
     * @return Thrift server max worker threads
     */
    int getThriftServerMaxWorkerThreads();

    /**
     * Thrift server client timeout.
     *
     * @return Thrift server client timeout
     */
    int getThriftServerSocketClientTimeoutInSeconds();

    /**
     * Epoch.
     *
     * @return epoch
     */
    boolean isEpochInSeconds();

    /**
     * Do we use pig types.
     *
     * @return Do we use pig types
     */
    boolean isUsePigTypes();

    /**
     * Max. number of threads for service.
     *
     * @return Max. number of threads for service
     */
    int getServiceMaxNumberOfThreads();

    /**
     * List of names for which the partition listing show throw error when no filter is specified.
     *
     * @return list of names
     */
    List<QualifiedName> getQualifiedNamesToThrowErrorWhenNoFilterOnListPartitions();

    /**
     * Elastic search index.
     *
     * @return elastic search index name
     */
    String getEsIndex();

    /**
     * Elastic search index.
     *
     * @return elastic search merge index name that's the new index to migrate to
     */
    String getMergeEsIndex();

    /**
     * Lifetime.
     *
     * @return lifetime
     */
    int getDataMetadataDeleteMarkerLifetimeInDays();

    /**
     * soft delete data metadata.
     *
     * @return true if we can delete data metadata
     */
    boolean canSoftDeleteDataMetadata();

    /**
     * cascade view and metadata delete on table delete.
     *
     * @return true if cascade
     */
    boolean canCascadeViewsMetadataOnTableDelete();

    /**
     * Max. number of in clause items in user metadata service queries.
     *
     * @return Max. number of in clause items in user metadata service queries
     */
    int getUserMetadataMaxInClauseItems();

    /**
     * Whether or not notifications should be published to SNS. If this is enabled the table topic ARN and
     * partition topic arn must also exist or SNS won't be enabled.
     *
     * @return Whether SNS notifications should be enabled
     */
    boolean isSnsNotificationEnabled();

    /**
     * Get the AWS ARN for the SNS topic to publish to for table related notifications.
     *
     * @return The table topic ARN or null if no property set
     */
    String getSnsTopicTableArn();

    /**
     * Get the AWS ARN for the SNS topic to publish to for partition related notifications.
     *
     * @return The partition topic ARN or null if no property set
     */
    String getSnsTopicPartitionArn();

    /**
     * Get the size of the thread pool used by SNS client.
     * @return size of the thread pool used by SNS client
     */
    int getSNSClientThreadCount();

    /**
     * Whether or not notifications should be published to SNS Partition topic. If this is enabled, the
     * partition topic arn must also exist or SNS won't be enabled.
     *
     * @return Whether SNS notifications to partitions topic should be enabled
     */
    boolean isSnsNotificationTopicPartitionEnabled();
}
