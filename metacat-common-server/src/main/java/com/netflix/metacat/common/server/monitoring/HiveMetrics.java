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

package com.netflix.metacat.common.server.monitoring;

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
    CounterHiveGetTablePartitionsTimeoutFailure(Type.counter,"getPartitionsTimeoutFailure"),
    CounterHiveExperimentGetTablePartitionsFailure(Type.counter,"experimentGetPartitionsFailure"),
    CounterHivePartitionPathIsNotDir(Type.counter,"partitionPathIsNotDir"),
    CounterHivePartitionFileSystemCall(Type.counter,"partitionFileSystemCall"),
    CounterHiveGetPartitionsExceedThresholdFailure(Type.counter,"getPartitionsExceedThresholdFailure"),
    CounterHiveFileSystemFailure(Type.counter,"fileSystemFailure"),
    CounterFileSystemReadFailure(Type.counter,"fileSystemReadFailure"),

    /**
     * Gauge.
     */
    GaugeAddPartitions(Type.gauge, "partitionAdd"),
    GaugeDeletePartitions(Type.gauge, "partitionDelete"),
    GaugeGetPartitionsCount(Type.gauge, "partitionGet"),
    GaugePreExpressionFilterGetPartitionsCount(Type.gauge, "preExpressionFilterGetPartitionsCount"),

    /**
     * Timer.
     */
    TimerHiveRequest(Type.timer, "embeddedclient.requests"),
    TimerFastHiveRequest(Type.timer, "fast.requests"),
    TimerExternalHiveRequest(Type.timer, "externalhms.requests"),
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
    TagGetTableNamesByFilter("getTableNamesByFilter"),
    TagGetTableByName("getTableByName"),
    TagLoadTable("loadTable"),
    TagAlterTable("alterTable"),
    TagAddPartitions("addPartitions"),
    TagAlterPartitions("alterPartitions"),
    TagCreatePartitionLocations("createPartitionLocations"),
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
        this.metricName = String.format("metacat.hive.%s.%s", type.name(), measure);
    }
    HiveMetrics(final String name) {
        this.metricName = name;
    }

    @Override
    public String toString() {
        return metricName;
    }
}
