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

package com.netflix.metacat.common.server.connector.iceberg;

import org.apache.iceberg.Table;
import lombok.Data;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the iceberg table.
 */
@Data
public class IcebergTableWrapper {
    /** Key for indicating if the table has non-main branches. */
    public static final String ICEBERG_HAS_NON_MAIN_BRANCHES_KEY = "iceberg.has.non.main.branches";
    /** Key for indicating if the table has tags. */
    public static final String ICEBERG_HAS_TAGS_KEY = "iceberg.has.tags";
    private final Table table;
    private final Map<String, String> extraProperties;

    /**
     * Check if the table has any non-main branches.
     * @return true if the table has branches other than main
     */
    public boolean hasNonMainBranches() {
        final Set<String> branches = extractBranches();
        return branches.size() > 1;
    }

    /**
     * Check if the table has any tags.
     * @return true if the table has tags
     */
    public boolean hasTags() {
        final Set<String> tags = extractTags();
        return !tags.isEmpty();
    }

    /**
     * Populate and return branch/tag metadata properties.
     * This should be called explicitly when metadata injection is needed.
     * @return map containing branch/tag metadata properties
     */
    public Map<String, String> populateBranchTagMetadata() {
        final Map<String, String> branchTagMetadata = new HashMap<>();
        branchTagMetadata.put(ICEBERG_HAS_NON_MAIN_BRANCHES_KEY, String.valueOf(hasNonMainBranches()));
        branchTagMetadata.put(ICEBERG_HAS_TAGS_KEY, String.valueOf(hasTags()));
        return branchTagMetadata;
    }

    /**
     * Get summary information about branches and tags for logging/debugging.
     * @return formatted string with branch and tag counts and names
     */
    public String getBranchesAndTagsSummary() {
        final Set<String> branches = extractBranches();
        final Set<String> tags = extractTags();
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
     * Extract branch names from the table references.
     * @return set of branch names
     * @throws RuntimeException if unable to read table references
     */
    private Set<String> extractBranches() {
        final var refs = table.refs();
        if (refs == null || refs.isEmpty()) {
            return Collections.emptySet();
        }
        return refs.keySet().stream()
            .filter(ref -> refs.get(ref).isBranch())
            .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * Extract tag names from the table references.
     * @return set of tag names
     * @throws RuntimeException if unable to read table references
     */
    private Set<String> extractTags() {
        final var refs = table.refs();
            if (refs == null || refs.isEmpty()) {
                return Collections.emptySet();
            }
        return refs.keySet().stream()
            .filter(ref -> refs.get(ref).isTag())
            .collect(java.util.stream.Collectors.toSet());
    }
}
