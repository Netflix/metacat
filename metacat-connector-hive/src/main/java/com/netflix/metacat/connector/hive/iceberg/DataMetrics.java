/*
 *  Copyright 2018 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.netflix.metacat.connector.hive.iceberg;

import lombok.Getter;

/**
 * Data Metrics.
 *
 * @author zhenl
 * @since 1.2.0
 */
@Getter
public enum DataMetrics {
    /**
     * number of rows.
     */
    rowCount("com.netflix.dse.mds.metric.RowCount"),
    /**
     * number of files.
     */
    fileCount("com.netflix.dse.mds.metric.NumFiles");


    private final String metricName;

    DataMetrics(final String name) {
        this.metricName = name;
    }

    @Override
    public String toString() {
        return metricName;
    }
}
