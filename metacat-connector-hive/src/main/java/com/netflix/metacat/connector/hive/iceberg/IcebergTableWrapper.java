/*
 *  Copyright 2019 Netflix, Inc.
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

package com.netflix.metacat.connector.hive.iceberg;

import org.apache.iceberg.Table;
import lombok.Getter;

import java.util.Map;
import java.util.Set;

/**
 * This class represents the iceberg table.
 */
@Getter
public class IcebergTableWrapper {
    private final Table table;
    private final Map<String, String> extraProperties;
    private final Set<String> branches;
    private final Set<String> tags;

    /**
     * Constructor for compatibility with existing code.
     * @param table the Iceberg table
     * @param extraProperties extra properties
     */
    public IcebergTableWrapper(final Table table, final Map<String, String> extraProperties) {
        this.table = table;
        this.extraProperties = extraProperties;
        this.branches = extractBranches();
        this.tags = extractTags();
    }

    /**
     * Check if the table has any branches (excluding the default main branch).
     * @return true if the table has branches other than main
     */
    public boolean hasBranches() {
        return !branches.isEmpty() && !(branches.size() == 1 && branches.contains("main"));
    }

    /**
     * Check if the table has any tags.
     * @return true if the table has tags
     */
    public boolean hasTags() {
        return !tags.isEmpty();
    }

    /**
     * Check if the table has any branches or tags (excluding the default main branch).
     * @return true if the table has branches (other than main) or tags
     */
    public boolean hasBranchesOrTags() {
        return hasBranches() || hasTags();
    }

    /**
     * Get summary information about branches and tags for logging/debugging.
     * @return formatted string with branch and tag counts and names
     */
    public String getBranchesAndTagsSummary() {
        final StringBuilder summary = new StringBuilder();
        summary.append(String.format("branches=%d", branches.size()));
        if (!branches.isEmpty()) {
            summary.append(String.format(" %s", branches));
        }
        summary.append(String.format(", tags=%d", tags.size()));
        if (!tags.isEmpty()) {
            summary.append(String.format(" %s", tags));
        }
        return summary.toString();
    }

    /**
     * Extract branch names from the table references using Iceberg 1.9 native API.
     * @return set of branch names
     */
    private Set<String> extractBranches() {
        try {
            return table.refs().keySet().stream()
                .filter(ref -> "branch".equals(table.refs().get(ref)))
                .collect(java.util.stream.Collectors.toSet());
        } catch (Exception e) {
            // Fallback to empty set if refs() is not supported or fails
            return java.util.Collections.emptySet();
        }
    }

    /**
     * Extract tag names from the table references using Iceberg 1.9 native API.
     * @return set of tag names
     */
    private Set<String> extractTags() {
        try {
            return table.refs().keySet().stream()
                .filter(ref -> "tag".equals(table.refs().get(ref)))
                .collect(java.util.stream.Collectors.toSet());
        } catch (Exception e) {
            // Fallback to empty set if refs() is not supported or fails
            return java.util.Collections.emptySet();
        }
    }
}
