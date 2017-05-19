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

package com.netflix.metacat.common.server.monitoring;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

//CHECKSTYLE:OFF
/**
 * Log constants.
 */
public enum LogConstants {
    /**
     * General logging constants.
     */
    AppPrefix("metacat"),

    /**
     * evnets.
     */
    CounterEventAsync(AppPrefix + ".events.count.Async"),
    CounterEventSync(AppPrefix + ".events.count.Sync"),

    /**
     * thrift request.
     */
    CounterThrift(AppPrefix + ".thrift.count.request"),

    /**
     * hive sql lock error.
     */
    CounterHiveSqlLockError(AppPrefix + ".count.hiveSqlLockError"),
    CounterHiveExperimentGetTablePartitionsFailure(AppPrefix + ".hive.count.experimentGetPartitionsFailure"),

    /**
     * metacat request.
     */
    CounterRequestCount(AppPrefix + ".count.request"),


    /**
     * Notifications.
     */
    CounterSNSNotificationPartitionAdd(AppPrefix + ".notifications.count.partitionsAdd"),
    CounterSNSNotificationTablePartitionAdd(AppPrefix + ".notifications.count.table.partitionsAdd"),
    CounterSNSNotificationPartitionDelete(AppPrefix + ".notifications.count.partitionsDelete"),
    CounterSNSNotificationTablePartitionDelete(AppPrefix + ".notifications.count.table.partitionsDelete"),
    CounterSNSNotificationTableCreate(AppPrefix + ".notifications.count.table.Create"),
    CounterSNSNotificationTableDelete(AppPrefix + ".notifications.count.table.Delete"),
    CounterSNSNotificationTableRename(AppPrefix + ".notifications.count.table.Rename"),
    CounterSNSNotificationTableUpdate(AppPrefix + ".notifications.count.table.Update"),

    /**
     * ElasticSearch.
     */
    CounterElasticSearchDatabaseCreate(AppPrefix + ".elasticsearch.count.databaseCreate"),
    CounterElasticSearchDatabaseDelete(AppPrefix + ".elasticsearch.count.databaseDelete"),
    CounterElasticSearchTableCreate(AppPrefix + ".elasticsearch.count.tableCreate"),
    CounterElasticSearchTableDelete(AppPrefix + ".elasticsearch.count.tableDelete"),
    CounterElasticSearchTableSave(AppPrefix + ".elasticsearch.count.tableSave"),
    CounterElasticSearchTableRename(AppPrefix + ".elasticsearch.count.tableRename"),
    CounterElasticSearchTableUpdate(AppPrefix + ".elasticsearch.count.tableUpdate"),
    CounterElasticSearchPartitionSave(AppPrefix + ".elasticsearch.count.partitionSave"),
    CounterElasticSearchPartitionDelete(AppPrefix + ".elasticsearch.count.partitionDelete"),
    CounterElasticSearchDelete(AppPrefix + ".elasticsearch.count.esDelete"),
    CounterElasticSearchBulkDelete(AppPrefix + ".elasticsearch.count.esBulkDelete"),
    CounterElasticSearchUpdate(AppPrefix + ".elasticsearch.count.esUpdate"),
    CounterElasticSearchBulkUpdate(AppPrefix + ".elasticsearch.count.esBulkUpdate"),
    CounterElasticSearchSave(AppPrefix + ".elasticsearch.count.esSave"),
    CounterElasticSearchBulkSave(AppPrefix + ".elasticsearch.count.esBulkSave"),
    CounterElasticSearchLog(AppPrefix + ".elasticsearch.count.esLog"),
    CounterElasticSearchRefresh(AppPrefix + ".elasticsearch.count.esRefresh"),
    CounterElasticSearchRefreshAlreadyRunning(AppPrefix + ".elasticsearch.count.esRefreshAlreadyRunning"),
    CounterElasticSearchUnmarkedDatabaseThreshholdReached(
            AppPrefix + ".elasticsearch.count.unmarkedDatabasesThresholdReached"),
    CounterElasticSearchUnmarkedTableThreshholdReached(
            AppPrefix + ".elasticsearch.count.unmarkedTablesThresholdReached"),

    /**
     * deleteMetadata.
     */
    CounterDeleteMetaData(AppPrefix + ".count.deleteMetadata"),

    /**
     * Gauges.
     */
    GaugeAddPartitions(AppPrefix + ".gauge.AddPartitions"),
    GaugeDeletePartitions(AppPrefix + ".gauge.DeletePartitions"),
    GaugeGetPartitionsCount(AppPrefix + ".gauge.GetPartitions"),

    GaugeConnectionsTotal(AppPrefix + ".connections.gauge.total"),
    GaugeConnectionsActive(AppPrefix + ".connections.gauge.active"),
    GaugeConnectionsIdle(AppPrefix + ".connections.gauge.idle"),

    /**
     * Timers.
     */
    TimerRequest(AppPrefix + ".timer.requests"),
    TimerThriftRequest(AppPrefix + ".thrift.timer.requests."),
    TimerHiveGetPartitions(AppPrefix + ".hive.timer.getPartitions"),
    TimerElasticSearchRefresh(AppPrefix + ".elasticsearch.timer.esRefresh"),

    /**
     * Status.
     */
    Status("status"), StatusSuccess("success"), StatusFailure("failure");

    private final String constant;

    LogConstants(final String constant) {
        this.constant = constant;
    }

    @Override
    public String toString() {
        return constant;
    }

    public static Map<String, String> getStatusSuccessMap() {
        return ImmutableMap.of(LogConstants.Status.name(), LogConstants.StatusSuccess.name());
    }

    public static Map<String, String> getStatusFailureMap() {
        return ImmutableMap.of(LogConstants.Status.name(), LogConstants.StatusFailure.name());
    }
}
