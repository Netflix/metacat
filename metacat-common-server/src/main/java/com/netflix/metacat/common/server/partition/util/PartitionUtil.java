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

package com.netflix.metacat.common.server.partition.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * Partition utility.
 */
@Slf4j
public final class PartitionUtil {
    /** Default partition value. */
    public static final String DEFAULT_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__";
    private static final Splitter EQUAL_SPLITTER = Splitter.on('=').trimResults();
    private static final Splitter SLASH_SPLITTER = Splitter.on('/').omitEmptyStrings().trimResults();

    private PartitionUtil() {
    }

    /**
     * Returns the partition key values from the given path.
     * @param location location path
     * @return the partition key values from the given path.
     */
    public static Map<String, String> getPartitionKeyValues(final String location) {
        final Map<String, String> parts = Maps.newLinkedHashMap();
        getPartitionKeyValues(location, parts);
        return parts;
    }

    /**
     * Sets the partition key values from the given path.
     * @param location location path
     * @param parts parts
     */
    public static void getPartitionKeyValues(final String location, final Map<String, String> parts) {
        for (String part : Splitter.on('/').omitEmptyStrings().split(location)) {
            if (part.contains("=")) {
                final String[] values = part.split("=", 2);
                //
                // Ignore the partition value, if it is null or the hive default. Hive sets the default value if the
                // value is null/empty string or any other values that cannot be escaped.
                //
                if (values[0].equalsIgnoreCase("null")
                    || values[1].equalsIgnoreCase("null")) {
                    log.debug("Found 'null' string in kvp [{}] skipping.", part);
                } else if (values[1].equalsIgnoreCase(DEFAULT_PARTITION_NAME)) {
                    parts.put(values[0], null);
                } else {
                    parts.put(values[0], values[1]);
                }
            }
        }
    }

    /**
     * Validates the partition name.
     * @param partitionName partition name
     * @param partitionKeys partition keys
     */
    public static void validatePartitionName(final String partitionName, final List<String> partitionKeys) {
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            throw new IllegalStateException("No partitionKeys are defined");
        }

        for (String part : SLASH_SPLITTER.split(partitionName)) {
            final List<String> tokens = EQUAL_SPLITTER.splitToList(part);
            if (tokens.size() != 2) {
                throw new IllegalArgumentException(String.format("Partition name '%s' is invalid", partitionName));
            }
            final String key = tokens.get(0);
            final String value = tokens.get(1);
            if (!partitionKeys.contains(key) || value.isEmpty() || "null".equalsIgnoreCase(value)) {
                throw new IllegalArgumentException(String.format("Partition name '%s' is invalid", partitionName));
            }
        }
    }
}
