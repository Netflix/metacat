/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.metacat.common.server.util;

import com.netflix.metacat.common.server.monitoring.Metrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.tomcat.jdbc.pool.ConnectionPool;
import org.apache.tomcat.jdbc.pool.JdbcInterceptor;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.apache.tomcat.jdbc.pool.PooledConnection;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pool stats interceptor.
 *
 * @author amajumdar
 */
public class PoolStatsInterceptor extends JdbcInterceptor {
    /**
     * Metric name.
     */
    public static final String PROP_METRIC_NAME = "name";
    private AtomicInteger metricNameTotalGauge;
    private AtomicInteger metricNameActiveGauge;
    private AtomicInteger metricNameIdleGauge;
    private final MeterRegistry registry = io.micrometer.core.instrument.Metrics.globalRegistry;

    /**
     * Constructor.
     */
    public PoolStatsInterceptor() {
        super();
    }

    @Override
    public void reset(final ConnectionPool parent, final PooledConnection con) {
        publishMetric(parent);
    }

    @Override
    public void disconnected(final ConnectionPool parent, final PooledConnection con, final boolean finalizing) {
        publishMetric(parent);
    }

    private void publishMetric(final ConnectionPool parent) {
        if (parent != null
            && metricNameTotalGauge != null
            && metricNameActiveGauge != null
            && metricNameIdleGauge != null) {
            metricNameTotalGauge.set(parent.getSize());
            metricNameActiveGauge.set(parent.getActive());
            metricNameIdleGauge.set(parent.getIdle());
        }
    }

    /**
     * Sets the metric.
     *
     * @param metricName metric name
     */
    public void setMetricName(final String metricName) {
        metricNameIdleGauge = registry.gauge(
            Metrics.GaugeConnectionsIdle + "." + metricName, new AtomicInteger(0));
        metricNameActiveGauge = registry.gauge(
            Metrics.GaugeConnectionsActive + "." + metricName, new AtomicInteger(0));
        metricNameTotalGauge = registry.gauge(
            Metrics.GaugeConnectionsTotal + "." + metricName, new AtomicInteger(0));
    }

    @Override
    public void setProperties(final Map<String, PoolProperties.InterceptorProperty> properties) {
        super.setProperties(properties);
        final PoolProperties.InterceptorProperty nameProperty = properties.get(PROP_METRIC_NAME);
        if (nameProperty != null) {
            setMetricName(nameProperty.getValue());
        }
    }
}
