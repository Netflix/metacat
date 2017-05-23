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
public enum Metrics {
    /**
     * General logging constants.
     */
    AppPrefix("metacat"),

    /**
     * Measure types
     */
    Count("count"),
    Gauge("gauge"),
    Timer("timer"),

    /**
     * evnets.
     */
    Events("events"),
    CounterEventAsync(Name(Events, Count , "async")),
    CounterEventSync(Name(Events, Count, "sync")),

    /**
     * thrift request.
     */
    Thrift("thrift"),
    CounterThrift(Name(Thrift, Count,   "request")),

    /**
     * hive sql lock error.
     */
    Hive("hive"),
    CounterHiveSqlLockError(Name(Hive, Count,"hiveSqlLockError")),
    CounterHiveExperimentGetTablePartitionsFailure(Name(Hive, Count,"experimentGetPartitionsFailure")),

    /**
     * metacat request.
     */
    Server("server"),
    CounterRequestCount(Name(Server,Count,"request")),
    CounterDeleteMetaData(Name(Server,Count,"deleteMetadata")),

    /**
     * Notifications.
     */
    Notifications("notifications"),
    CounterSNSNotificationPartitionAdd(Name(Notifications,Count,"partitionsAdd")),
    CounterSNSNotificationTablePartitionAdd(Name(Notifications,Count,"table.partitionsAdd")),
    CounterSNSNotificationPartitionDelete(Name(Notifications,Count,"partitionsDelete")),
    CounterSNSNotificationTablePartitionDelete(Name(Notifications,Count,"table.partitionsDelete")),
    CounterSNSNotificationTableCreate(Name(Notifications,Count,"table.Create")),
    CounterSNSNotificationTableDelete(Name(Notifications,Count,"table.Delete")),
    CounterSNSNotificationTableRename(Name(Notifications,Count,"table.Rename")),
    CounterSNSNotificationTableUpdate(Name(Notifications,Count,"table.Update")),

    /**
     * ElasticSearch.
     */
    ElasticSearch("elasticsearch"),
    CounterElasticSearchDatabaseCreate(Name(ElasticSearch,Count,"databaseCreate")),
    CounterElasticSearchDatabaseDelete(Name(ElasticSearch,Count,"databaseDelete")),
    CounterElasticSearchTableCreate(Name(ElasticSearch,Count,"tableCreate")),
    CounterElasticSearchTableDelete(Name(ElasticSearch,Count,"tableDelete")),
    CounterElasticSearchTableSave(Name(ElasticSearch,Count,"tableSave")),
    CounterElasticSearchTableRename(Name(ElasticSearch,Count,"tableRename")),
    CounterElasticSearchTableUpdate(Name(ElasticSearch,Count,"tableUpdate")),
    CounterElasticSearchPartitionSave(Name(ElasticSearch,Count,"partitionSave")),
    CounterElasticSearchPartitionDelete(Name(ElasticSearch,Count,"partitionDelete")),
    CounterElasticSearchDelete(Name(ElasticSearch,Count,"esDelete")),
    CounterElasticSearchBulkDelete(Name(ElasticSearch,Count,"esBulkDelete")),
    CounterElasticSearchUpdate(Name(ElasticSearch,Count,"esUpdate")),
    CounterElasticSearchBulkUpdate(Name(ElasticSearch,Count,"esBulkUpdate")),
    CounterElasticSearchSave(Name(ElasticSearch,Count,"esSave")),
    CounterElasticSearchBulkSave(Name(ElasticSearch,Count,"esBulkSave")),
    CounterElasticSearchLog(Name(ElasticSearch,Count,"esLog")),
    CounterElasticSearchRefresh(Name(ElasticSearch,Count,"esRefresh")),
    CounterElasticSearchRefreshAlreadyRunning(Name(ElasticSearch,Count,"esRefreshAlreadyRunning")),
    CounterElasticSearchUnmarkedDatabaseThreshholdReached(Name(ElasticSearch,Count,"unmarkedDatabasesThresholdReached")),
    CounterElasticSearchUnmarkedTableThreshholdReached(Name(ElasticSearch,Count,"unmarkedTablesThresholdReached")),

    /**
     * Gauges.
     */
    GaugeAddPartitions(Name(Hive, Gauge,"partitionAdd")),
    GaugeDeletePartitions(Name(Hive, Gauge,"partitionDelete")),
    GaugeGetPartitionsCount(Name(Hive, Gauge,"partitionGet")),

    /**
     * Jdbc Interceptor
     */
    JdbcInterceptor("jdbcinterceptor"),
    GaugeConnectionsTotal(Name(JdbcInterceptor, Gauge,"connections.total")),
    GaugeConnectionsActive(Name(JdbcInterceptor, Gauge,"connections.active")),
    GaugeConnectionsIdle(Name(JdbcInterceptor, Gauge,"connections.idle")),

    /**
     * Timers.
     */
    TimerRequest(Name(Server, Timer,"requests")),
    TimerHiveRequest(Name(Hive, Timer,"embeddedclient.requests")),
    TimerFastHiveRequest(Name(Hive, Timer,"fast.requests")),
    TimerThriftRequest(Name(Thrift, Timer,"requests")),
    TimerElasticSearchRefresh(Name(ElasticSearch, Timer,"esRefresh")),

    /**
     * Status.
     */
    Status("status"), StatusSuccess("success"), StatusFailure("failure");

    private final String constant;

    Metrics(final String constant) {
        this.constant = constant;
    }

    @Override
    public String toString() {
        return constant;
    }

    public static Map<String, String> getStatusSuccessMap() {
        return ImmutableMap.of(Metrics.Status.name(), Metrics.StatusSuccess.name());
    }

    public static Map<String, String> getStatusFailureMap() {
        return ImmutableMap.of(Metrics.Status.name(), Metrics.StatusFailure.name());
    }

    private static String Name(final Metrics component, final Metrics type, final String measure) {
        return AppPrefix + "." + component.name() + "." + type.name() + "." + measure;
    }
}
