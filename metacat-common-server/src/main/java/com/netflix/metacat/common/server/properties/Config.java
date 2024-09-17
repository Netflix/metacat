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
import java.util.Map;
import java.util.Set;

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
     * If true, a table update should trigger updating data metadata of all table documents containing the same uri.
     *
     * @return true, if data metadata of all table documents needs to be updated if their uri is the same
     * as the updated table.
     */
    boolean isElasticSearchUpdateTablesWithSameUriEnabled();

    /**
     * Enable publishing partitions to elastic search.
     *
     * @return true if publishing partitions to elastic search is enabled
     */
    boolean isElasticSearchPublishPartitionEnabled();

    /**
     * Enable publishing metacat es error logs to elastic search.
     *
     * @return true if publishing metacat es error logs to elastic search is enabled
     */
    boolean isElasticSearchPublishMetacatLogEnabled();

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
     * Elastic search call timeout.
     *
     * @return elastic search call timeout
     */
    long getElasticSearchCallTimeout();

    /**
     * Elastic search bulk call timeout.
     *
     * @return elastic search bulk call timeout
     */
    long getElasticSearchBulkCallTimeout();

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
     * If true, will escape partition names when filtering partitions.
     *
     * @return Whether to escape partition names when filtering partitions
     */
    boolean escapePartitionNameOnFilter();

    /**
     * Number of records to fetch in one call from Hive Metastore.
     *
     * @return fetch size
     */
    int getHiveMetastoreFetchSize();

    /**
     * Number of records to save in one call to Hive Metastore.
     *
     * @return batch size
     */
    int getHiveMetastoreBatchSize();

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
    List<QualifiedName> getNamesToThrowErrorOnListPartitionsWithNoFilter();

    /**
     * Threshold for list of partitions returned.
     *
     * @return Threshold for list of partitions returned
     */
    int getMaxPartitionsThreshold();

    /**
     * Threshold for list of partitions to be added.
     *
     * @return Threshold for list of partitions to be added
     */
    int getMaxAddedPartitionsThreshold();

    /**
     * Threshold for list of partitions to be deleted.
     *
     * @return Threshold for list of partitions to be deleted
     */
    int getMaxDeletedPartitionsThreshold();

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
     * Get the fallback AWS ARN for the SNS topic to publish to for table related notifications.
     *
     * @return The table topic ARN or null if no property set
     */
    String getFallbackSnsTopicTableArn();

    /**
     * Get the fallback AWS ARN for the SNS topic to publish to for partition related notifications.
     *
     * @return The partition topic ARN or null if no property set
     */
    String getFallbackSnsTopicPartitionArn();

    /**
     * Whether or not notifications should be published to SNS Partition topic. If this is enabled, the
     * partition topic arn must also exist or SNS won't be enabled.
     *
     * @return Whether SNS notifications to partitions topic should be enabled
     */
    boolean isSnsNotificationTopicPartitionEnabled();

    /**
     * Enable attaching the partition ids in the payload.
     *
     * @return whether the partition ids in the payload.
     */
    boolean isSnsNotificationAttachPartitionIdsEnabled();

    /**
     * The number of max partition ids in the payload.
     *
     * @return The number of max partition ids in the payload.
     */
    int getSnsNotificationAttachPartitionIdMax();

    /**
     * Whether or not to delete definition metadata for tables.
     *
     * @return Whether or not to delete definition metadata for tables.
     */
    boolean canDeleteTableDefinitionMetadata();

    /**
     * List of names for which the table definition metadata will be deleted.
     *
     * @return list of names
     */
    Set<QualifiedName> getNamesEnabledForDefinitionMetadataDelete();

    /**
     * Enable cache.
     *
     * @return true if cache is enabled
     */
    boolean isCacheEnabled();

    /**
     * Enable authorization.
     *
     * @return true if authorization is enabled
     */
    boolean isAuthorizationEnabled();

    /**
     * Get the metacat create acl property.
     *
     * @return The metacat create acl property
     */
    Map<QualifiedName, Set<String>> getMetacatCreateAcl();

    /**
     * Get the metacat delete acl property.
     *
     * @return The metacat delete acl property
     */
    Map<QualifiedName, Set<String>> getMetacatDeleteAcl();

    /**
     * get Iceberg Table Summary Fetch Size.
     *
     * @return Iceberg Table Summary Fetch Size
     */
    int getIcebergTableSummaryFetchSize();
    /**
     * Enable iceberg table processing.
     *
     * @return true if iceberg table processing is enabled
     */
    boolean isIcebergEnabled();
    /**
     * Enable iceberg table cache.
     *
     * @return true if iceberg table cache is enabled
     */
    boolean isIcebergCacheEnabled();
    /**
     * Enable iceberg table TableMetadata cache.
     *
     * @return true if iceberg table cache is enabled
     */
    boolean isIcebergTableMetadataCacheEnabled();

    /**
     * Enable common view processing.
     *
     * @return true if common view processing is enabled
     */
    boolean isCommonViewEnabled();

    /**
     * Whether the underlying storage table should be deleted
     * for a materialized common view.
     *
     * @return true if the storage table should be deleted when the common view is.
     */
    boolean deleteCommonViewStorageTable();

    /**
     * get Iceberg table refresh metadata location retry number.
     *
     * @return Refresh metadata location retry number.
     */
    int getIcebergRefreshFromMetadataLocationRetryNumber();

    /**
     * get Iceberg table max metadata file size allowed in metacat.
     *
     * @return Refresh Iceberg table max metadata file size.
     */
    long getIcebergMaxMetadataFileSize();

    /**
     * get Iceberg partition uri scheme.
     *
     * @return Iceberg table partition uri scheme.
     */
    String getIcebergPartitionUriScheme();

    /**
     * Whether the table alias is enabled.
     *
     * @return True if it is.
     */
    boolean isTableAliasEnabled();

    /**
     * Set of tags that disable table delete.
     *
     * @return set of tags
     */
    Set<String> getNoTableDeleteOnTags();

    /**
     * Set of tags that disable table rename.
     *
     * @return set of tags
     */
    Set<String> getNoTableRenameOnTags();

    /**
     * Set of tags that disable table update.
     *
     * @return set of tags
     */
    Set<String> getNoTableUpdateOnTags();

    /**
     * Whether the rate limiter is enabled.
     *
     * @return True if it is.
     */
    boolean isRateLimiterEnabled();

    /**
     * Whether the rate limiter is enforced
     * and rejecting requests.
     *
     * @return True if it is.
     */
    boolean isRateLimiterEnforced();

    /**
     * Whether the update iceberg table post event handler
     * is enabled.
     *
     * @return True if it is.
     */
    boolean isUpdateIcebergTableAsyncPostEventEnabled();

    /**
     * Whether to list table names by default on getDatabase request call.
     *
     * @return True if it is.
     */
    boolean listTableNamesByDefaultOnGetDatabase();

    /**
     * Whether to list database names by default on getCatalog request call.
     *
     * @return True if it is.
     */
    boolean listDatabaseNameByDefaultOnGetCatalog();

    /**
     * Get the page size when listing table entities.
     *
     * @return size of the page
     */
    int getListTableEntitiesPageSize();

    /**
     * Get the page size when listing table names.
     *
     * @return size of the page
     */
    int getListTableNamesPageSize();

    /**
     * Get the page size when listing db entities.
     *
     * @return size of the page
     */
    int getListDatabaseEntitiesPageSize();

    /**
     * Get the page size when listing db names.
     *
     * @return size of the page
     */
    int getListDatabaseNamesPageSize();

    /**
     * Metadata query timeout in seconds.
     *
     * @return Metadata query timeout in seconds
     */
    int getMetadataQueryTimeout();

    /**
     * Long metadata query timeout in seconds, for longer running queries.
     *
     * @return Long metadata query timeout in seconds
     */
    int getLongMetadataQueryTimeout();

    /**
     * Whether to check the existence of the iceberg metadata location before updating the table.
     *
     * @return Whether to check the existence of the iceberg metadata location before updating the table
     */
    boolean isIcebergPreviousMetadataLocationCheckEnabled();

    /**
     * Whether partition definition metadata should be disabled.
     *
     * @return True if it should be blocked.
     */
    boolean disablePartitionDefinitionMetadata();

    /**
     * Whether the request flag to only fetch the iceberg metadata location should be respected.
     *
     * @return True if it should be.
     */
    boolean shouldFetchOnlyMetadataLocationEnabled();

    /**
     * Whether we allow parent child relationship to be created.
     *
     * @return True if it should be.
     */
    boolean isParentChildCreateEnabled();

    /**
     * Whether we allow renaming parent child relationship.
     *
     * @return True if it should be.
     */
    boolean isParentChildRenameEnabled();

    /**
     * Whether we allow getting parent child relationship in the getTable call.
     *
     * @return True if it should be.
     */
    boolean isParentChildGetEnabled();

    /**
     * Whether we allow dropping tables that are either parent or child.
     *
     * @return True if it should be.
     */
    boolean isParentChildDropEnabled();

    /**
     * Get the parentChildRelationshipProperties config.
     *
     * @return parentChildRelationshipProperties
     */
    ParentChildRelationshipProperties getParentChildRelationshipProperties();
}

