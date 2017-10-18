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
    CounterHiveSqlLockError(Type.counter, "hiveSqlLockError"),
    CounterHiveExperimentGetTablePartitionsFailure(Type.counter,"experimentGetPartitionsFailure"),
    CounterHivePartitionPathIsNotDir(Type.counter,"partitionPathIsNotDir"),

    /**
     * Gauge.
     */
    GaugeAddPartitions(Type.gauge, "partitionAdd"),
    GaugeDeletePartitions(Type.gauge, "partitionDelete"),
    GaugeGetPartitionsCount(Type.gauge, "partitionGet"),

    /**
     * Timer.
     */
    TimerHiveRequest(Type.timer, "embeddedclient.requests"), TimerFastHiveRequest(Type.timer, "fast.requests"),
    /**
     * hive function names.
     */
    TagCreateDatabase("createDatabase"),
    TagCreateTable("createTable"),
    TagDropDatabase("dropDatabase"),
    TagDropHivePartitions("dropHivePartitions"),
    TagAlterDatabase("alterDatabase"),
    TagGetAllDatabases("getAllDatabases"),
    TagGetDatabase("getDatabase"),
    TagGetAllTables("getAllTables"),
    TagGetTableByName("getTableByName"),
    TagLoadTable("loadTable"),
    TagAlterTable("alterTable"),
    TagAddPartitions("addPartitions"),
    TagAlterPartitions("alterPartitions"),
    TagAddDropPartitions("addDropPartitions"),
    TagDropTable("dropTable"),
    TagRename("rename"),
    TagListPartitionsByFilter("listPartitionsByFilter"),
    TagGetPartitions("getPartitions"),
    TagGetPartitionCount("getPartitionCount"),
    TagGetPartitionKeys("getPartitionKeys"),
    TagGetPartitionNames("getPartitionNames"),
    TagGetTableNames("getTableNames"),
    TagTableExists("tableExists");

    enum Type {
        counter,
        gauge,
        timer
    }

    private final String metricName;

    HiveMetrics(final Type type, final String measure) {
        this.metricName = "metacat.hive." + type.name() + "." + type.name() + "." + measure;
    }
    HiveMetrics(final String name) {
        this.metricName = name;
    }

    @Override
    public String toString() {
        return metricName;
    }
}
