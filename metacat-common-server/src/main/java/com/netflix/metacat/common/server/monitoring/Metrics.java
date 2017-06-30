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
 * Metric measures.
 *
 * @author zhenl
 * @since 1.0.0
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
     * events.
     */
    Events("events"),
    CounterEventAsync(Name(Events, Count, "async")),
    CounterEventSync(Name(Events, Count, "sync")),

    /**
     * thrift request.
     */
    Thrift("thrift"),
    CounterThrift(Name(Thrift, Count, "request")),

    /**
     * Gauge.
     */
    PartionService("partitionservice"),
    GaugeAddPartitions(Name(PartionService, Metrics.Gauge, "partitionAdd")),
    GaugeDeletePartitions(Name(PartionService, Metrics.Gauge, "partitionDelete")),
    GaugeGetPartitionsCount(Name(PartionService, Metrics.Gauge, "partitionGet")),

    /**
     * metacat request.
     */
    Server("server"),
    CounterRequestCount(Name(Server,Count,"request")),
    CounterRequestFailureCount(Name(Server,Count,"requestfailure")),
    CounterDeleteMetaData(Name(Server,Count,"deleteMetadata")),

    /**
     * Notifications.
     */
    Notifications("notifications"),
    CounterSNSNotificationPartitionAdd(Name(Notifications, Count, "partitionsAdd")),
    CounterSNSNotificationTablePartitionAdd(Name(Notifications, Count, "table.partitionsAdd")),
    CounterSNSNotificationPartitionDelete(Name(Notifications, Count, "partitionsDelete")),
    CounterSNSNotificationTablePartitionDelete(Name(Notifications, Count, "table.partitionsDelete")),
    CounterSNSNotificationTableCreate(Name(Notifications, Count, "table.Create")),
    CounterSNSNotificationTableDelete(Name(Notifications, Count, "table.Delete")),
    CounterSNSNotificationTableRename(Name(Notifications, Count, "table.Rename")),
    CounterSNSNotificationTableUpdate(Name(Notifications, Count, "table.Update")),
    CounterSNSNotificationPublishMessageSizeExceeded(Name(Notifications, Count, "publish.message.size.exceeded")),

    /**
     * ElasticSearch.
     */
    ElasticSearch("elasticsearch"),
    TimerElasticSearchEventsDelay(Name(ElasticSearch, Timer, "events.delay")),
    TimerElasticSearchDatabaseCreate(Name(ElasticSearch, Timer, "databaseCreate")),
    TimerElasticSearchDatabaseDelete(Name(ElasticSearch, Timer, "databaseDelete")),
    TimerElasticSearchTableCreate(Name(ElasticSearch, Timer, "tableCreate")),
    TimerElasticSearchTableDelete(Name(ElasticSearch, Timer, "tableDelete")),
    TimerElasticSearchTableSave(Name(ElasticSearch, Timer, "tableSave")),
    TimerElasticSearchTableRename(Name(ElasticSearch, Timer, "tableRename")),
    TimerElasticSearchTableUpdate(Name(ElasticSearch, Timer, "tableUpdate")),
    TimerElasticSearchPartitionSave(Name(ElasticSearch, Timer, "partitionSave")),
    TimerElasticSearchPartitionDelete(Name(ElasticSearch, Timer, "partitionDelete")),
    CounterElasticSearchDelete(Name(ElasticSearch, Count, "esDelete")),
    CounterElasticSearchBulkDelete(Name(ElasticSearch, Count, "esBulkDelete")),
    CounterElasticSearchUpdate(Name(ElasticSearch, Count, "esUpdate")),
    CounterElasticSearchBulkUpdate(Name(ElasticSearch, Count, "esBulkUpdate")),
    CounterElasticSearchSave(Name(ElasticSearch, Count, "esSave")),
    CounterElasticSearchBulkSave(Name(ElasticSearch, Count, "esBulkSave")),
    CounterElasticSearchLog(Name(ElasticSearch, Count, "esLog")),
    CounterElasticSearchRefresh(Name(ElasticSearch, Count, "esRefresh")),
    CounterElasticSearchRefreshAlreadyRunning(Name(ElasticSearch, Count, "esRefreshAlreadyRunning")),
    CounterElasticSearchUnmarkedDatabaseThreshholdReached(Name(ElasticSearch, Count, "unmarkedDatabasesThresholdReached")),
    CounterElasticSearchUnmarkedTableThreshholdReached(Name(ElasticSearch, Count, "unmarkedTablesThresholdReached")),

    /**
     * Jdbc Interceptor
     */
    JdbcInterceptor("jdbcinterceptor"),
    GaugeConnectionsTotal(Name(JdbcInterceptor, Gauge, "connections.total")),
    GaugeConnectionsActive(Name(JdbcInterceptor, Gauge, "connections.active")),
    GaugeConnectionsIdle(Name(JdbcInterceptor, Gauge, "connections.idle")),

    /**
     * Timers.
     */
    TimerRequest(Name(Server, Timer, "requests")),
    TimerThriftRequest(Name(Thrift, Timer, "requests")),
    TimerElasticSearchRefresh(Name(ElasticSearch, Timer, "esRefresh")),
    TimerNotificationsPublishDelay(Name(Notifications, Timer, "publish.delay")),
    TimerNotificationsBeforePublishDelay(Name(Notifications, Timer, "before.publish.delay")),

    /**
     * Status.
     */
    Status("status"), StatusSuccess("success"), StatusFailure("failure");

    public final static Map<String, String> statusSuccessMap
        = ImmutableMap.of(Metrics.Status.name(), Metrics.StatusSuccess.name());
    public final static Map<String, String> statusFailureMap
        = ImmutableMap.of(Metrics.Status.name(), Metrics.StatusFailure.name());

    private final String constant;

    Metrics(final String constant) {
        this.constant = constant;
    }

    private static String Name(final Metrics component, final Metrics type, final String measure) {
        return AppPrefix + "." + component.name() + "." + type.name() + "." + measure;
    }

    @Override
    public String toString() {
        return constant;
    }
}
