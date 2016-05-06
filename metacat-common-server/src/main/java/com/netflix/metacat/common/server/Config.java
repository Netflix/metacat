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
    int getThriftMaxWorkerThreadsSize();
    int getThriftRequestTimeoutInSeconds();
    boolean isEpochInSeconds();
    boolean isUsePigTypes();
    int getServiceMaxNumberOfThreads();
    List<QualifiedName> getQualifiedNamesToThrowErrorWhenNoFilterOnListPartitions();
}
