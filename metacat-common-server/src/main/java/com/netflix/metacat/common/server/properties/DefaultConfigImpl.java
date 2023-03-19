/*
 *
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
 *
 */
package com.netflix.metacat.common.server.properties;

import com.netflix.metacat.common.QualifiedName;
import lombok.NonNull;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A default implementation of the config interface.
 *
 * @author tgianos
 * @since 1.1.0
 */
public class DefaultConfigImpl implements Config {

    private final MetacatProperties metacatProperties;

    /**
     * Constructor.
     *
     * @param metacatProperties The metacat properties to use
     */
    public DefaultConfigImpl(@Nonnull @NonNull final MetacatProperties metacatProperties) {
        this.metacatProperties = metacatProperties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDefaultTypeConverter() {
        return this.metacatProperties.getType().getConverter();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isElasticSearchEnabled() {
        return this.metacatProperties.getElasticsearch().isEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isElasticSearchUpdateTablesWithSameUriEnabled() {
        return this.metacatProperties.getElasticsearch().getPublish().isUpdateTablesWithSameUriEnabled();
    }

    @Override
    public boolean isElasticSearchPublishPartitionEnabled() {
        return this.metacatProperties.getElasticsearch().getPublish().isPartitionEnabled();
    }

    @Override
    public boolean isElasticSearchPublishMetacatLogEnabled() {
        return this.metacatProperties.getElasticsearch().getPublish().isMetacatLogEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getElasticSearchClusterName() {
        return this.metacatProperties.getElasticsearch().getCluster().getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getElasticSearchClusterNodes() {
        return this.metacatProperties.getElasticsearch().getCluster().getNodes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getElasticSearchClusterPort() {
        return this.metacatProperties.getElasticsearch().getCluster().getPort();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getElasticSearchScrollFetchSize() {
        return this.metacatProperties.getElasticsearch().getScroll().getFetch().getSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getElasticSearchScrollTimeout() {
        return this.metacatProperties.getElasticsearch().getScroll().getTimeout().getMs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getElasticSearchCallTimeout() {
        return this.metacatProperties.getElasticsearch().getTimeout();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getElasticSearchBulkCallTimeout() {
        return this.metacatProperties.getElasticsearch().getBulkTimeout();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> getElasticSearchRefreshExcludeQualifiedNames() {
        return this.metacatProperties
            .getElasticsearch()
            .getRefresh()
            .getExclude()
            .getQualified()
            .getNamesAsListOfQualifiedNames();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getElasticSearchRefreshIncludeCatalogs() {
        return this.metacatProperties.getElasticsearch().getRefresh().getInclude().getCatalogs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> getElasticSearchRefreshIncludeDatabases() {
        return this.metacatProperties.getElasticsearch().getRefresh().getInclude().getDatabasesAsListOfQualfiedNames();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getElasticSearchRefreshPartitionsIncludeCatalogs() {
        return this.metacatProperties.getElasticsearch().getRefresh().getPartitions().getInclude().getCatalogs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getElasticSearchThresholdUnmarkedDatabasesDelete() {
        return this.metacatProperties
            .getElasticsearch()
            .getRefresh()
            .getThreshold()
            .getUnmarked()
            .getDatabases()
            .getDelete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getElasticSearchThresholdUnmarkedTablesDelete() {
        return this.metacatProperties
            .getElasticsearch()
            .getRefresh()
            .getThreshold()
            .getUnmarked()
            .getTables()
            .getDelete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getEventBusExecutorThreadCount() {
        return this.metacatProperties.getEvent().getBus().getExecutor().getThread().getCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getEventBusThreadCount() {
        return this.metacatProperties.getEvent().getThread().getCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHivePartitionWhitelistPattern() {
        return this.metacatProperties.getHive().getMetastore().getPartition().getName().getWhitelist().getPattern();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean escapePartitionNameOnFilter() {
        return this.metacatProperties.getHive().getMetastore().getPartition().isEscapeNameOnFilter();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHiveMetastoreFetchSize() {
        return this.metacatProperties.getHive().getMetastore().getFetchSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHiveMetastoreBatchSize() {
        return this.metacatProperties.getHive().getMetastore().getBatchSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLookupServiceUserAdmin() {
        return this.metacatProperties.getLookupService().getUserAdmin();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMetacatVersion() {
        // TODO
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getPluginConfigLocation() {
        return this.metacatProperties.getPlugin().getConfig().getLocation();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTagServiceUserAdmin() {
        return this.metacatProperties.getTagService().getUserAdmin();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getThriftServerMaxWorkerThreads() {
        return this.metacatProperties.getThrift().getServerMaxWorkerThreads();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getThriftServerSocketClientTimeoutInSeconds() {
        return this.metacatProperties.getThrift().getServerSocketClientTimeoutInSeconds();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEpochInSeconds() {
        return this.metacatProperties.getType().isEpochInSeconds();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isUsePigTypes() {
        return this.metacatProperties.getFranklin().getConnector().getUse().getPig().isType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getServiceMaxNumberOfThreads() {
        return this.metacatProperties.getService().getMax().getNumber().getThreads();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<QualifiedName> getNamesToThrowErrorOnListPartitionsWithNoFilter() {
        return this.metacatProperties
            .getService()
            .getTables()
            .getError()
            .getList()
            .getPartitions()
            .getNo()
            .getFilterAsListOfQualifiedNames();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxPartitionsThreshold() {
        return this.metacatProperties
            .getService()
            .getTables()
            .getError()
            .getList()
            .getPartitions()
            .getThreshold();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxAddedPartitionsThreshold() {
        return this.metacatProperties
            .getService()
            .getTables()
            .getError()
            .getList()
            .getPartitions()
            .getAddThreshold();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxDeletedPartitionsThreshold() {
        return this.metacatProperties
            .getService()
            .getTables()
            .getError()
            .getList()
            .getPartitions()
            .getDeleteThreshold();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getEsIndex() {
        return this.metacatProperties.getElasticsearch().getIndex().getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getMergeEsIndex() {
        return this.metacatProperties.getElasticsearch().getMergeIndex().getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getDataMetadataDeleteMarkerLifetimeInDays() {
        return this.metacatProperties.getData().getMetadata().getDelete().getMarker().getLifetime().getDays();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canSoftDeleteDataMetadata() {
        return this.metacatProperties.getUser().getMetadata().isSoftDelete();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canCascadeViewsMetadataOnTableDelete() {
        return this.metacatProperties.getTable().getDelete().getCascade().getViews().isMetadata();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUserMetadataMaxInClauseItems() {
        return this.metacatProperties.getUser().getMetadata().getMaxInClauseItems();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSnsNotificationEnabled() {
        return this.metacatProperties.getNotifications().getSns().isEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSnsTopicTableArn() {
        return this.metacatProperties.getNotifications().getSns().getTopic().getTable().getArn();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSnsTopicPartitionArn() {
        return this.metacatProperties.getNotifications().getSns().getTopic().getPartition().getArn();
    }

    @Override
    public String getFallbackSnsTopicTableArn() {
        return this.metacatProperties.getNotifications().getSns().getTopic().getTable().getFallbackArn();
    }

    @Override
    public String getFallbackSnsTopicPartitionArn() {
        return this.metacatProperties.getNotifications().getSns().getTopic().getPartition().getFallbackArn();
    }

    @Override
    public boolean isSnsNotificationTopicPartitionEnabled() {
        return this.metacatProperties.getNotifications().getSns().getTopic().getPartition().isEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSnsNotificationAttachPartitionIdsEnabled() {
        return this.metacatProperties.getNotifications().getSns().getAttachPartitionIds().isEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getSnsNotificationAttachPartitionIdMax() {
        return this.metacatProperties.getNotifications().getSns().getAttachPartitionIds().getMaxPartitionIdNumber();
    }

    @Override
    public boolean canDeleteTableDefinitionMetadata() {
        return this.metacatProperties.getDefinition().getMetadata().getDelete().isEnableForTable();
    }

    @Override
    public Set<QualifiedName> getNamesEnabledForDefinitionMetadataDelete() {
        return this.metacatProperties.getDefinition().getMetadata().getDelete().getQualifiedNamesEnabledForDelete();
    }

    @Override
    public boolean isCacheEnabled() {
        return this.metacatProperties.getCache().isEnabled();
    }


    @Override
    public boolean isAuthorizationEnabled() {
        return this.metacatProperties.getAuthorization().isEnabled();
    }

    @Override
    public Map<QualifiedName, Set<String>> getMetacatCreateAcl() {
        return this.metacatProperties.getAuthorization().getCreateAcl().getCreateAclMap();
    }

    @Override
    public Map<QualifiedName, Set<String>> getMetacatDeleteAcl() {
        return this.metacatProperties.getAuthorization().getDeleteAcl().getDeleteAclMap();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int getIcebergTableSummaryFetchSize() {
        return this.metacatProperties.getHive().getIceberg().getFetchSizeInTableSummary();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isIcebergEnabled() {
        return this.metacatProperties.getHive().getIceberg().isEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isIcebergCacheEnabled() {
        return this.metacatProperties.getHive().getIceberg().getCache().isEnabled();
    }

    @Override
    public boolean isIcebergTableMetadataCacheEnabled() {
        return this.metacatProperties.getHive().getIceberg().getCache().getMetadata().isEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCommonViewEnabled() {
        return this.metacatProperties.getHive().getCommonview().isEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteCommonViewStorageTable() {
        return this.metacatProperties.getHive().getCommonview().isDeleteStorageTable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getIcebergRefreshFromMetadataLocationRetryNumber() {
        return this.metacatProperties.getHive().getIceberg().getRefreshFromMetadataLocationRetryNumber();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getIcebergMaxMetadataFileSize() {
        return this.metacatProperties.getHive().getIceberg().getMaxMetadataFileSizeBytes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIcebergPartitionUriScheme() {
        return this.metacatProperties.getHive().getIceberg().getPartitionUriScheme();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTableAliasEnabled() {
        return this.metacatProperties.getAliasServiceProperties().isEnabled();
    }

    @Override
    public Set<String> getNoTableDeleteOnTags() {
        return this.metacatProperties.getTable().getDelete().getNoDeleteOnTagsSet();
    }

    @Override
    public Set<String> getNoTableRenameOnTags() {
        return this.metacatProperties.getTable().getRename().getNoRenameOnTagsSet();
    }

    @Override
    public Set<String> getNoTableUpdateOnTags() {
        return this.metacatProperties.getTable().getUpdate().getNoUpdateOnTagsSet();
    }

    /**
     * Whether the rate limiter is enabled.
     *
     * @return True if it is.
     */
    @Override
    public boolean isRateLimiterEnabled() {
        return this.metacatProperties.getRateLimiterProperties().isEnabled();
    }

    /**
     * Whether the rate limiter is being enforced
     * and rejecting requests.
     *
     * @return True if it is.
     */
    public boolean isRateLimiterEnforced() {
        return this.metacatProperties.getRateLimiterProperties().isEnforced();
    }

    @Override
    public boolean isUpdateIcebergTableAsyncPostEventEnabled() {
        return this.metacatProperties.getEvent().isUpdateIcebergTableAsyncPostEventEnabled();
    }

    @Override
    public boolean listTableNamesByDefaultOnGetDatabase() {
        return this.metacatProperties.getService().isListTableNamesByDefaultOnGetDatabase();
    }

    @Override
    public boolean listDatabaseNameByDefaultOnGetCatalog() {
        return this.metacatProperties.getService().isListDatabaseNameByDefaultOnGetCatalog();
    }

    @Override
    public int getMetadataQueryTimeout() {
        return this.metacatProperties.getUsermetadata().getQueryTimeoutInSeconds();
    }

    @Override
    public boolean isIcebergPreviousMetadataLocationCheckEnabled() {
        return this.metacatProperties.getHive().getIceberg().isIcebergPreviousMetadataLocationCheckEnabled();
    }

    @Override
    public boolean disablePartitionDefinitionMetadata() {
        return this.metacatProperties.getDefinition().getMetadata().isDisablePartitionDefinitionMetadata();
    }

    @Override
    public boolean shouldFetchOnlyMetadataLocationEnabled() {
        return this.metacatProperties.getHive().getIceberg().isShouldFetchOnlyMetadataLocationEnabled();
    }
}
