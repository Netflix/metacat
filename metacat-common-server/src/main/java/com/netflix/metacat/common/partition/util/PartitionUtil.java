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

package com.netflix.metacat.common.partition.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by amajumdar on 5/7/15.
 */
public class PartitionUtil {
    private static final Splitter EQUAL_SPLITTER = Splitter.on('=').trimResults();
    private static final Splitter SLASH_SPLITTER = Splitter.on('/').omitEmptyStrings().trimResults();
    private static final Logger log = LoggerFactory.getLogger(PartitionUtil.class);
    public static Map<String, String> getPartitionKeyValues(String location) {
        Map<String, String> parts = Maps.newLinkedHashMap();
        getPartitionKeyValues(location, parts);
        return parts;
    }

    public static void getPartitionKeyValues(String location, Map<String, String> parts) {
        for (String part : Splitter.on('/').omitEmptyStrings().split(location)) {
            if (part.contains("=")) {
                String[] values = part.split("=", 2);

                if(values[0].equalsIgnoreCase("null") || values[1].equalsIgnoreCase("null")) {
                    log.debug("Found 'null' string in kvp [{}] skipping.", part);
                } else {
                    parts.put(values[0], values[1]);
                }
            }
        }
    }

    public static void validatePartitionName(String partitionName, List<String> partitionKeys) {
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            throw new IllegalStateException("No partitionKeys are defined");
        }

        for (String part : SLASH_SPLITTER.split(partitionName)) {
            List<String> tokens = EQUAL_SPLITTER.splitToList(part);
            if (tokens.size() != 2) {
                throw new IllegalArgumentException(String.format("Partition name '%s' is invalid", partitionName));
            }
            String key = tokens.get(0);
            String value = tokens.get(1);
            if (!partitionKeys.contains(key) || value.isEmpty() || "null".equalsIgnoreCase(value)) {
                throw new IllegalArgumentException(String.format("Partition name '%s' is invalid", partitionName));
            }
        }
    }
}
