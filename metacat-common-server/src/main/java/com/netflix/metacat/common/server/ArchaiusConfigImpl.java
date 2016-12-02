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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.metacat.common.QualifiedName;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Fast property configurations.
 */
public class ArchaiusConfigImpl implements Config {
    private final DynamicStringProperty defaultTypeConverter;
    private final DynamicBooleanProperty isElasticSearchEnabled;
    private final DynamicStringProperty elasticSearchIndexName;
    private final DynamicBooleanProperty elasticsearchMigration;
    private final DynamicStringProperty elasticsearchMergeIndexName;
    private final DynamicStringProperty elasticSearchClusterName;
    private final DynamicStringProperty elasticSearchClusterNodes;
    private final DynamicIntProperty elasticSearchClusterPort;
    private final DynamicStringProperty elasticSearchRefreshExcludeQualifiedNames;
    private final DynamicStringProperty elasticSearchRefreshIncludeCatalogs;
    private final DynamicStringProperty elasticSearchRefreshIncludeDatabases;
    private final DynamicStringProperty elasticSearchRefreshPartitionsIncludeCatalogs;
    private final DynamicIntProperty elasticSearchScrollFetchSize;
    private final DynamicIntProperty elasticSearchScrollTimeout;
    private final DynamicIntProperty elasticSearchThresholdUnmarkedDatabasesDelete;
    private final DynamicIntProperty elasticSearchThresholdUnmarkedTablesDelete;
    private final DynamicBooleanProperty epochInSeconds;
    private final DynamicIntProperty eventBusExecutorThreadCount;
    private final DynamicIntProperty eventBusThreadCount;
    private final DynamicStringProperty hivePartitionWhitelistPattern;
    private final DynamicStringProperty lookupServiceUserAdmin;
    private final DynamicStringProperty pluginConfigLocation;
    private final DynamicStringProperty tagServiceUserAdmin;
    private final DynamicIntProperty thriftServerMaxWorkerThreads;
    private final DynamicIntProperty thriftServerSocketClientTimeoutInSeconds;
    private final DynamicStringProperty metacatVersion;
    private final DynamicBooleanProperty usePigTypes;
    private final DynamicIntProperty serviceMaxNumberOfThreads;
    private final DynamicStringProperty tableNamesToThrowErrorWhenNoFilterOnListPartitions;
    private List<QualifiedName> qualifiedNamesToThrowErrorWhenNoFilterOnListPartitions;
    private List<QualifiedName> qualifiedNamesElasticSearchRefreshExclude;
    private List<QualifiedName> qualifiedNamesElasticSearchRefreshIncludeDatabases;
    private final DynamicIntProperty dataMetadataDeleteMarkerLifetimeInDays;
    private final DynamicBooleanProperty canSoftDeleteDataMetadata;
    private final DynamicBooleanProperty canCascadeViewsMetadataOnTableDelete;
    private final DynamicIntProperty userMetadataMaxInClauseItems;

    /**
     * Default constructor.
     */
    public ArchaiusConfigImpl() {
        this(DynamicPropertyFactory.getInstance());
    }

    /**
     * Constructor.
     * @param factory property factory
     */
    public ArchaiusConfigImpl(final DynamicPropertyFactory factory) {
        this.defaultTypeConverter = factory
            .getStringProperty("metacat.type.converter", "com.netflix.metacat.converters.impl.PrestoTypeConverter");
        this.isElasticSearchEnabled = factory.getBooleanProperty("metacat.elacticsearch.enabled", true);
        this.elasticSearchIndexName = factory.getStringProperty("metacat.elacticsearch.index.name", "metacat");
        this.elasticsearchMigration = factory.getBooleanProperty("metacat.elacticsearch.migration.enabled", false);
        this.elasticsearchMergeIndexName =
            factory.getStringProperty("metacat.elacticsearch.mergeindex.name", "metacat");
        this.elasticSearchClusterName = factory.getStringProperty("metacat.elacticsearch.cluster.name", null);
        this.elasticSearchClusterNodes = factory.getStringProperty("metacat.elacticsearch.cluster.nodes", null);
        this.elasticSearchClusterPort = factory.getIntProperty("metacat.elacticsearch.cluster.port", 7102);
        this.elasticSearchRefreshIncludeCatalogs = factory
            .getStringProperty("metacat.elacticsearch.refresh.include.catalogs", null);
        this.elasticSearchRefreshPartitionsIncludeCatalogs = factory
            .getStringProperty("metacat.elacticsearch.refresh.partitions.include.catalogs",
                "prodhive,testhive,s3,aegisthus");
        this.elasticSearchScrollFetchSize = factory.getIntProperty("metacat.elacticsearch.scroll.fetch.size", 50000);
        this.elasticSearchScrollTimeout = factory.getIntProperty("metacat.elacticsearch.scroll.timeout.ms", 600000);
        this.elasticSearchThresholdUnmarkedDatabasesDelete = factory
            .getIntProperty("metacat.elacticsearch.refresh.threshold.unmarked.databases.delete", 100);
        this.elasticSearchThresholdUnmarkedTablesDelete = factory
            .getIntProperty("metacat.elacticsearch.refresh.threshold.unmarked.tables.delete", 1000);
        this.epochInSeconds = factory.getBooleanProperty("metacat.type.epoch_in_seconds", true);
        this.eventBusExecutorThreadCount = factory.getIntProperty("metacat.event.bus.executor.thread.count", 10);
        this.eventBusThreadCount = factory.getIntProperty("metacat.event.thread.count", 10);
        this.hivePartitionWhitelistPattern = factory
            .getStringProperty("metacat.hive.metastore.partition.name.whitelist.pattern", "");
        this.lookupServiceUserAdmin = factory.getStringProperty("metacat.lookup_service.user_admin", "admin");
        this.metacatVersion = factory.getStringProperty("netflix.appinfo.version", "1.0.0");
        this.pluginConfigLocation = factory.getStringProperty("metacat.plugin.config.location", null);
        this.tagServiceUserAdmin = factory.getStringProperty("metacat.tag_service.user_admin", "admin");
        this.thriftServerMaxWorkerThreads = factory.getIntProperty("metacat.thrift.server_max_worker_threads", 200);
        this.thriftServerSocketClientTimeoutInSeconds = factory
            .getIntProperty("metacat.thrift.server_socket_client_timeout_in_seconds", 60);
        this.usePigTypes = factory.getBooleanProperty("metacat.franklin.connector.use.pig.type", true);
        this.serviceMaxNumberOfThreads = factory.getIntProperty("metacat.service.max.number.threads", 50);
        this.tableNamesToThrowErrorWhenNoFilterOnListPartitions = factory.getStringProperty(
            "metacat.service.tables.error.list.partitions.no.filter",
            null, this::setQualifiedNamesToThrowErrorWhenNoFilterOnListPartitions);
        setQualifiedNamesToThrowErrorWhenNoFilterOnListPartitions();
        this.elasticSearchRefreshExcludeQualifiedNames = factory
            .getStringProperty("metacat.elacticsearch.refresh.exclude.qualified.names", null,
                this::setQualifiedNamesToElasticSearchRefreshExcludeQualifiedNames);
        setQualifiedNamesToElasticSearchRefreshExcludeQualifiedNames();
        this.elasticSearchRefreshIncludeDatabases = factory
            .getStringProperty("metacat.elacticsearch.refresh.include.databases", null,
                this::setQualifiedNamesToElasticSearchRefreshIncludeDatabases);
        setQualifiedNamesToElasticSearchRefreshIncludeDatabases();
        this.dataMetadataDeleteMarkerLifetimeInDays = factory
            .getIntProperty("metacat.data.metadata.delete.marker.lifetime.days", 15);
        this.canSoftDeleteDataMetadata = factory.getBooleanProperty("metacat.user.metadata.soft_delete", true);
        this.canCascadeViewsMetadataOnTableDelete = factory
            .getBooleanProperty("metacat.table.delete.cascade.views.metadata", true);
        this.userMetadataMaxInClauseItems = factory.getIntProperty("metacat.user.metadata.max_in_clause_items", 2500);
    }

    private void setQualifiedNamesToElasticSearchRefreshExcludeQualifiedNames() {
        final String qNames = elasticSearchRefreshExcludeQualifiedNames.get();
        if (!Strings.isNullOrEmpty(qNames)) {
            qualifiedNamesElasticSearchRefreshExclude = Splitter.on(',').omitEmptyStrings()
                .splitToList(qNames).stream()
                .map(QualifiedName::fromString).collect(Collectors.toList());
        } else {
            qualifiedNamesElasticSearchRefreshExclude = Lists.newArrayList();
        }
    }

    private void setQualifiedNamesToElasticSearchRefreshIncludeDatabases() {
        final String databaseNames = elasticSearchRefreshIncludeDatabases.get();
        if (!Strings.isNullOrEmpty(databaseNames)) {
            qualifiedNamesElasticSearchRefreshIncludeDatabases = Splitter.on(',').omitEmptyStrings()
                .splitToList(databaseNames).stream()
                .map(QualifiedName::fromString).collect(Collectors.toList());
        } else {
            qualifiedNamesElasticSearchRefreshIncludeDatabases = Lists.newArrayList();
        }
    }

    private void setQualifiedNamesToThrowErrorWhenNoFilterOnListPartitions() {
        final String tableNames = tableNamesToThrowErrorWhenNoFilterOnListPartitions.get();
        if (!Strings.isNullOrEmpty(tableNames)) {
            qualifiedNamesToThrowErrorWhenNoFilterOnListPartitions = Splitter.on(',').omitEmptyStrings()
                .splitToList(tableNames).stream()
                .map(QualifiedName::fromString).collect(Collectors.toList());
        } else {
            qualifiedNamesToThrowErrorWhenNoFilterOnListPartitions = Lists.newArrayList();
        }
    }

    @Override
    public String getDefaultTypeConverter() {
        return defaultTypeConverter.get();
    }

    @Override
    public boolean isElasticSearchEnabled() {
        return isElasticSearchEnabled.get();
    }

    @Override
    public String getElasticSearchClusterName() {
        return elasticSearchClusterName.get();
    }

    @Override
    public String getElasticSearchClusterNodes() {
        return elasticSearchClusterNodes.get();
    }

    @Override
    public int getElasticSearchClusterPort() {
        return elasticSearchClusterPort.get();
    }

    @Override
    public List<QualifiedName> getElasticSearchRefreshExcludeQualifiedNames() {
        return qualifiedNamesElasticSearchRefreshExclude;
    }

    @Override
    public String getElasticSearchRefreshIncludeCatalogs() {
        return elasticSearchRefreshIncludeCatalogs.get();
    }

    @Override
    public List<QualifiedName> getElasticSearchRefreshIncludeDatabases() {
        return qualifiedNamesElasticSearchRefreshIncludeDatabases;
    }

    @Override
    public String getElasticSearchRefreshPartitionsIncludeCatalogs() {
        return elasticSearchRefreshPartitionsIncludeCatalogs.get();
    }

    @Override
    public int getElasticSearchScrollFetchSize() {
        return elasticSearchScrollFetchSize.get();
    }

    @Override
    public int getElasticSearchScrollTimeout() {
        return elasticSearchScrollTimeout.get();
    }

    @Override
    public int getElasticSearchThresholdUnmarkedDatabasesDelete() {
        return elasticSearchThresholdUnmarkedDatabasesDelete.get();
    }

    @Override
    public int getElasticSearchThresholdUnmarkedTablesDelete() {
        return elasticSearchThresholdUnmarkedTablesDelete.get();
    }

    @Override
    public int getEventBusExecutorThreadCount() {
        return eventBusExecutorThreadCount.get();
    }

    @Override
    public int getEventBusThreadCount() {
        return eventBusThreadCount.get();
    }

    @Override
    public String getHivePartitionWhitelistPattern() {
        return hivePartitionWhitelistPattern.get();
    }

    @Override
    public String getLookupServiceUserAdmin() {
        return lookupServiceUserAdmin.get();
    }

    @Override
    public String getMetacatVersion() {
        return metacatVersion.get();
    }

    @Override
    public String getPluginConfigLocation() {
        return pluginConfigLocation.get();
    }

    @Override
    public String getTagServiceUserAdmin() {
        return tagServiceUserAdmin.get();
    }

    @Override
    public int getThriftServerMaxWorkerThreads() {
        return thriftServerMaxWorkerThreads.get();
    }

    @Override
    public int getThriftServerSocketClientTimeoutInSeconds() {
        return thriftServerSocketClientTimeoutInSeconds.get();
    }

    @Override
    public boolean isEpochInSeconds() {
        return epochInSeconds.get();
    }

    @Override
    public boolean isUsePigTypes() {
        return usePigTypes.get();
    }

    @Override
    public int getServiceMaxNumberOfThreads() {
        return serviceMaxNumberOfThreads.get();
    }

    @Override
    public List<QualifiedName> getQualifiedNamesToThrowErrorWhenNoFilterOnListPartitions() {
        return qualifiedNamesToThrowErrorWhenNoFilterOnListPartitions;
    }

    @Override
    public String getEsIndex() {
        return elasticSearchIndexName.get();
    }

    @Override
    public int getDataMetadataDeleteMarkerLifetimeInDays() {
        return dataMetadataDeleteMarkerLifetimeInDays.get();
    }

    @Override
    public boolean canSoftDeleteDataMetadata() {
        return canSoftDeleteDataMetadata.get();
    }

    @Override
    public boolean canCascadeViewsMetadataOnTableDelete() {
        return canCascadeViewsMetadataOnTableDelete.get();
    }

    @Override
    public int getUserMetadataMaxInClauseItems() {
        return userMetadataMaxInClauseItems.get();
    }

    @Override
    public String getMergeEsIndex() {
        return elasticsearchMergeIndexName.get();
    }

    @Override
    public boolean isIndexMigration() {
        return elasticsearchMigration.get();
    }
}
