/*
 *       Copyright 2017 Netflix, Inc.
 *          Licensed under the Apache License, Version 2.0 (the "License");
 *          you may not use this file except in compliance with the License.
 *          You may obtain a copy of the License at
 *              http://www.apache.org/licenses/LICENSE-2.0
 *          Unless required by applicable law or agreed to in writing, software
 *          distributed under the License is distributed on an "AS IS" BASIS,
 *          WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *          See the License for the specific language governing permissions and
 *          limitations under the License.
 */

package com.netflix.metacat.main.services.search;

import com.netflix.metacat.common.server.monitoring.Metrics;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import lombok.Getter;

/**
 * Elastic Search Metric.
 *
 * @author zhenl
 * @since 1.1.0
 */
@Getter
public class ElasticSearchMetric {
    private final Counter elasticSearchDeleteFailureCounter;
    private final Counter elasticSearchBulkDeleteFailureCounter;
    private final Counter elasticSearchUpdateFailureCounter;
    private final Counter elasticSearchBulkUpdateFailureCounter;
    private final Counter elasticSearchLogFailureCounter;
    private final Counter elasticSearchSaveFailureCounter;
    private final Counter elasticSearchBulkSaveFailureCounter;

    /**
     * ElasticSearchMetric constructor.
     *
     * @param registry spectator registry
     */
    public ElasticSearchMetric(final Registry registry) {
        this.elasticSearchDeleteFailureCounter = registry.counter(
            registry.createId(Metrics.CounterElasticSearchDelete.getMetricName())
                .withTags(Metrics.tagStatusFailureMap));
        this.elasticSearchBulkDeleteFailureCounter = registry.counter(
            registry.createId(
                Metrics.CounterElasticSearchBulkDelete.getMetricName())
                .withTags(Metrics.tagStatusFailureMap));
        this.elasticSearchUpdateFailureCounter = registry.counter(
            registry.createId(Metrics.CounterElasticSearchUpdate.getMetricName())
                .withTags(Metrics.tagStatusFailureMap));
        this.elasticSearchBulkUpdateFailureCounter = registry.counter(
            registry.createId(
                Metrics.CounterElasticSearchBulkUpdate.getMetricName()).withTags(Metrics.tagStatusFailureMap));
        this.elasticSearchLogFailureCounter = registry.counter(
            registry.createId(Metrics.CounterElasticSearchLog.getMetricName()).withTags(Metrics.tagStatusFailureMap));
        this.elasticSearchSaveFailureCounter = registry.counter(
            registry.createId(Metrics.CounterElasticSearchSave.getMetricName()).withTags(Metrics.tagStatusFailureMap));
        this.elasticSearchBulkSaveFailureCounter = registry.counter(
            registry.createId(Metrics.CounterElasticSearchBulkSave.getMetricName())
                .withTags(Metrics.tagStatusFailureMap));
    }
}
