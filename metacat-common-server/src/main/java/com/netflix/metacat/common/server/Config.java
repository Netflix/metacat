package com.netflix.metacat.common.server;

public interface Config {
    String getDefaultTypeConverter();
    String getElasticSearchClusterName();
    String getElasticSearchClusterNodes();
    int getElasticSearchClusterPort();
    int getElasticSearchScrollFetchSize();
    int getElasticSearchScrollTimeout();
    String getElasticSearchRefreshExcludeDatabases();
    String getElasticSearchRefreshIncludeCatalogs();
    String getElasticSearchRefreshIncludeDatabases();
    String getElasticSearchRefreshPartitionsIncludeCatalogs();
    int getElasticSearchThresholdUnmarkedDatabasesDelete();
    int getElasticSearchThresholdUnmarkedTablesDelete();
    int getEventBusExecutorThreadCount();
    int getEventBusThreadCount();
    String getHivePartitionWhitelistPattern();
    String getLookupServiceUserAdmin();
    String getMetacatVersion();
    String getPluginConfigLocation();
    String getTagServiceUserAdmin();
    boolean isEpochInSeconds();
    boolean isUsePigTypes();
}
