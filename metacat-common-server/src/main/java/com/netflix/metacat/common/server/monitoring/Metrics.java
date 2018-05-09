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
import lombok.Getter;

import java.util.Map;

//CHECKSTYLE:OFF

/**
 * Metric measures.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Getter
public enum Metrics {
    /**
     * Events.
     */
    CounterEventPublish(Component.events, Type.counter, "publish"),

    /**
     * thrift request.
     */
    CounterThrift(Component.thrift, Type.counter, "request"),

    /**
     * DistributionSummary.
     */
    DistributionSummaryAddPartitions(Component.partionservice, Type.distributionSummary, "partitionAdd"),
    DistributionSummaryMetadataOnlyAddPartitions(Component.partionservice, Type.distributionSummary, "partitionMetadataOnlyAdd"),
    DistributionSummaryGetPartitions(Component.partionservice, Type.distributionSummary, "partitionGet"),
    DistributionSummaryDeletePartitions(Component.partionservice, Type.distributionSummary, "partitionDelete"),
    /**
     * metacat request.
     */
    CounterRequestCount(Component.server, Type.counter, "request"),
    CounterRequestFailureCount(Component.server, Type.counter, "requestfailure"),
    CounterDeleteMetaData(Component.server, Type.counter, "deleteMetadata"),

    /**
     * Notifications.
     */
    CounterSNSNotificationPartitionAdd(Component.notifications, Type.counter, "partitionsAdd"),
    CounterSNSNotificationPartitionLatestDeleteColumnAdd(Component.notifications, Type.counter, "partitionsLatestDeleteColumnAdd"),
    CounterSNSNotificationTablePartitionAdd(Component.notifications, Type.counter, "table.partitionsAdd"),
    CounterSNSNotificationPartitionDelete(Component.notifications, Type.counter, "partitionsDelete"),
    CounterSNSNotificationTablePartitionDelete(Component.notifications, Type.counter, "table.partitionsDelete"),
    CounterSNSNotificationTableCreate(Component.notifications, Type.counter, "table.Create"),
    CounterSNSNotificationTableDelete(Component.notifications, Type.counter, "table.Delete"),
    CounterSNSNotificationTableRename(Component.notifications, Type.counter, "table.Rename"),
    CounterSNSNotificationTableUpdate(Component.notifications, Type.counter, "table.Update"),
    CounterSNSNotificationPublishMessageSizeExceeded(Component.notifications, Type.counter, "publish.message.size.exceeded"),
    CounterSNSNotificationPublishFallback(Component.notifications, Type.counter, "publish.fallback"),
    CounterSNSNotificationPublishPartitionIdNumberExceeded(Component.notifications, Type.counter, "publish.partitionid.number.exceeded"),

    /**
     * ElasticSearch.
     */
    TimerElasticSearchEventsDelay(Component.elasticsearch, Type.timer, "events.delay"),
    TimerElasticSearchDatabaseCreate(Component.elasticsearch, Type.timer, "databaseCreate"),
    TimerElasticSearchDatabaseDelete(Component.elasticsearch, Type.timer, "databaseDelete"),
    TimerElasticSearchTableCreate(Component.elasticsearch, Type.timer, "tableCreate"),
    TimerElasticSearchTableDelete(Component.elasticsearch, Type.timer, "tableDelete"),
    TimerElasticSearchTableSave(Component.elasticsearch, Type.timer, "tableSave"),
    TimerElasticSearchTableRename(Component.elasticsearch, Type.timer, "tableRename"),
    TimerElasticSearchTableUpdate(Component.elasticsearch, Type.timer, "tableUpdate"),
    TimerElasticSearchPartitionSave(Component.elasticsearch, Type.timer, "partitionSave"),
    TimerElasticSearchPartitionDelete(Component.elasticsearch, Type.timer, "partitionDelete"),
    CounterElasticSearchDelete(Component.elasticsearch, Type.counter, "esDelete"),
    CounterElasticSearchBulkDelete(Component.elasticsearch, Type.counter, "esBulkDelete"),
    CounterElasticSearchUpdate(Component.elasticsearch, Type.counter, "esUpdate"),
    CounterElasticSearchBulkUpdate(Component.elasticsearch, Type.counter, "esBulkUpdate"),
    CounterElasticSearchSave(Component.elasticsearch, Type.counter, "esSave"),
    CounterElasticSearchBulkSave(Component.elasticsearch, Type.counter, "esBulkSave"),
    CounterElasticSearchLog(Component.elasticsearch, Type.counter, "esLog"),
    CounterElasticSearchRefresh(Component.elasticsearch, Type.counter, "esRefresh"),
    CounterElasticSearchRefreshAlreadyRunning(Component.elasticsearch, Type.counter, "esRefreshAlreadyRunning"),
    CounterElasticSearchUnmarkedDatabaseThreshholdReached(Component.elasticsearch, Type.counter, "unmarkedDatabasesThresholdReached"),
    CounterElasticSearchUnmarkedTableThreshholdReached(Component.elasticsearch, Type.counter, "unmarkedTablesThresholdReached"),


    /**
     * Jdbc Interceptor
     */
    GaugeConnectionsTotal(Component.jdbcinterceptor, Type.gauge, "connections.total"),
    GaugeConnectionsActive(Component.jdbcinterceptor, Type.gauge, "connections.active"),
    GaugeConnectionsIdle(Component.jdbcinterceptor, Type.gauge, "connections.idle"),

    /**
     * Timers.
     */
    TimerRequest(Component.server, Type.timer, "requests"),
    TimerThriftRequest(Component.server, Type.timer, "requests"),
    TimerElasticSearchRefresh(Component.server, Type.timer, "esRefresh"),
    TimerNotificationsPublishDelay(Component.server, Type.timer, "publish.delay"),
    TimerNotificationsBeforePublishDelay(Component.server, Type.timer, "before.publish.delay"),
    TimerSavePartitionMetadata(Component.partionservice, Type.timer, "savePartitionMetadata"),
    TimerSaveTableMetadata(Component.tableservice, Type.timer, "saveTableMetadata"),

    TagEventsType("metacat.events.type");

    public final static Map<String, String> tagStatusSuccessMap
        = ImmutableMap.of("status", "success");
    public final static Map<String, String> tagStatusFailureMap
        = ImmutableMap.of("status", "failure");



    enum Type {
        counter,
        gauge,
        timer,
        distributionSummary
    }

    enum Component {
        metacat,
        events,
        thrift,
        server,
        notifications,
        tableservice,
        partionservice,
        elasticsearch,
        jdbcinterceptor
    }

    private final String metricName;

    Metrics(final Component component, final Type type, final String measure) {
        this.metricName = Component.metacat.name() + "." + component.name() + "." + type.name() + "." + measure;
    }

    Metrics(final String tagName) {
        this.metricName = tagName;
    }

    @Override
    public String toString() {
        return metricName;
    }
}
