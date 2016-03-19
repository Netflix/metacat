package com.netflix.metacat.common.server;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;

public class ArchaiusConfigImpl implements Config {
    private final DynamicStringProperty defaultTypeConverter;
    private final DynamicStringProperty elasticSearchClusterName;
    private final DynamicStringProperty elasticSearchClusterNodes;
    private final DynamicIntProperty elasticSearchClusterPort;
    private final DynamicStringProperty elasticSearchRefreshExcludeDatabases;
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
    private final DynamicStringProperty metacatVersion;
    private final DynamicBooleanProperty usePigTypes;

    public ArchaiusConfigImpl() {
        this(DynamicPropertyFactory.getInstance());
    }

    public ArchaiusConfigImpl(DynamicPropertyFactory factory) {
        this.defaultTypeConverter = factory
                .getStringProperty("metacat.type.converter", "com.netflix.metacat.converters.impl.PrestoTypeConverter");
        this.elasticSearchClusterName = factory.getStringProperty("metacat.elacticsearch.cluster.name", null);
        this.elasticSearchClusterNodes = factory.getStringProperty("metacat.elacticsearch.cluster.nodes", null);
        this.elasticSearchClusterPort = factory.getIntProperty("metacat.elacticsearch.cluster.port", 7102);
        this.elasticSearchRefreshExcludeDatabases = factory
                .getStringProperty("metacat.elacticsearch.refresh.exclude.databases", null);
        this.elasticSearchRefreshIncludeCatalogs = factory
                .getStringProperty("metacat.elacticsearch.refresh.include.catalogs", null);
        this.elasticSearchRefreshIncludeDatabases = factory
                .getStringProperty("metacat.elacticsearch.refresh.include.databases", null);
        this.elasticSearchRefreshPartitionsIncludeCatalogs = factory
                .getStringProperty("metacat.elacticsearch.refresh.partitions.include.catalogs",
                        "prodhive,testhive,s3,aegisthus");
        this.elasticSearchScrollFetchSize = factory.getIntProperty("metacat.elacticsearch.scroll.fetch.size", 500);
        this.elasticSearchScrollTimeout = factory.getIntProperty("metacat.elacticsearch.scroll.timeout.ms", 60000);
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
        this.usePigTypes = factory.getBooleanProperty("metacat.franklin.connector.use.pig.type", true);
    }

    @Override
    public String getDefaultTypeConverter() {
        return defaultTypeConverter.get();
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
    public String getElasticSearchRefreshExcludeDatabases() {
        return elasticSearchRefreshExcludeDatabases.get();
    }

    @Override
    public String getElasticSearchRefreshIncludeCatalogs() {
        return elasticSearchRefreshIncludeCatalogs.get();
    }

    @Override
    public String getElasticSearchRefreshIncludeDatabases() {
        return elasticSearchRefreshIncludeDatabases.get();
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
    public boolean isEpochInSeconds() {
        return epochInSeconds.get();
    }

    @Override
    public boolean isUsePigTypes() {
        return usePigTypes.get();
    }
}
