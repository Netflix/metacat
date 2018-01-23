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
    public int getSNSClientThreadCount() {
        return this.metacatProperties.getNotifications().getSns().getThreadCount();
    }

    @Override
    public boolean isSnsNotificationTopicPartitionEnabled() {
        return this.metacatProperties.getNotifications().getSns().getTopic().getPartition().isEnabled();
    }

    @Override
    public boolean canDeleteTableDefinitionMetadata() {
        return this.metacatProperties.getDefinition().getMetadata().getDelete().isEnableForTable();
    }

    @Override
    public Set<QualifiedName> getNamesEnabledForDefinitionMetadataDelete() {
        return this.metacatProperties.getDefinition().getMetadata().getDelete().getQualifiedNamesEnabledForDelete();
    }
}
