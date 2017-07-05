/*
 *  Copyright 2017 Netflix, Inc.
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
 */
//CHECKSTYLE:OFF

package com.netflix.metacat.connector.hive.monitoring;

import com.netflix.metacat.common.server.monitoring.Metrics;
import lombok.Getter;

/**
 * Hive Metrics.
 *
 * @author zhenl
 * @since 1.0.0
 */
@Getter
public enum HiveMetrics {

    /**
     * hive sql lock error.
     */
    Hive("hive"),
    CounterHiveSqlLockError(Name(Hive, Metrics.Count, "hiveSqlLockError")),
    CounterHiveExperimentGetTablePartitionsFailure(Name(Hive, Metrics.Count, "experimentGetPartitionsFailure")),

    /**
     * Gauge.
     */
    GaugeAddPartitions(Name(Hive, Metrics.Gauge, "partitionAdd")),
    GaugeDeletePartitions(Name(Hive, Metrics.Gauge, "partitionDelete")),
    GaugeGetPartitionsCount(Name(Hive, Metrics.Gauge, "partitionGet")),

    /**
     * Timer.
     */
    TimerHiveRequest(Name(Hive, Metrics.Timer, "embeddedclient.requests")), TimerFastHiveRequest(Name(Hive, Metrics.Timer, "fast.requests")),

    /**
     * hive function names.
     */
    createDatabase("createDatabase"),
    createTable("createTable"),
    dropDatabase("dropDatabase"),
    dropHivePartitions("dropHivePartitions"),
    alterDatabase("alterDatabase"),
    getAllDatabases("getAllDatabases"),
    getDatabase("getDatabase"),
    getAllTables("getAllTables"),
    getTableByName("getTableByName"),
    loadTable("loadTable"),
    alterTable("alterTable"),
    alterPartitions("alterPartitions"),
    addDropPartitions("addDropPartitions"),
    dropTable("dropTable"),
    rename("rename"),
    listPartitionsByFilter("listPartitionsByFilter"),
    getPartitions("getPartitions"),
    getPartitionCount("getPartitionCount"),
    getPartitionKeys("getPartitionKeys"),
    getPartitionNames("getPartitionNames"),
    getTableNames("getTableNames"),
    exists("exists");

    private final String metricName;

    HiveMetrics(final String constant) {
        this.metricName = constant;
    }

    @Override
    public String toString() {
        return metricName;
    }

    private static String Name(final HiveMetrics component, final Metrics type, final String measure) {
        return Metrics.AppPrefix + "." + component.name() + "." + type.name() + "." + measure;
    }
}
