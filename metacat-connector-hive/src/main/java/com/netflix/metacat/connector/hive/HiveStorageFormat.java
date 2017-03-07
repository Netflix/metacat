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

package com.netflix.metacat.connector.hive;

import javax.annotation.Nonnull;


/**
 * Hive storage format.
 *
 * @author zhenl
 */
public enum HiveStorageFormat {
    /**
     * Optimized Row Columnar.
     */
    ORC("org.apache.hadoop.hive.ql.io.orc.OrcSerde",
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
    /**
     * PARQUET.
     */
    PARQUET("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),

    /**
     * RCBINARY.
     */
    RCBINARY("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe",
            "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
            "org.apache.hadoop.hive.ql.io.RCFileOutputFormat"),

    /**
     * RCTEXT.
     */
    RCTEXT("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe",
            "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
            "org.apache.hadoop.hive.ql.io.RCFileOutputFormat"),

    /**
     * SEQUENCEFILE.
     */
    SEQUENCEFILE("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "org.apache.hadoop.mapred.SequenceFileInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),

    /**
     * TEXTFILE.
     */
    TEXTFILE("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.mapred.TextInputFormat");

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;

    HiveStorageFormat(@Nonnull final String serde,
                      @Nonnull final String inputFormat,
                      @Nonnull final String outputFormat) {
        this.serde = serde;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
    }

    public String getSerDe() {
        return serde;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

}
