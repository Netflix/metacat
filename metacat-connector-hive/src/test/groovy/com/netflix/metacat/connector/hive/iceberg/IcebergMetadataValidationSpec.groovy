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
 */

package com.netflix.metacat.connector.hive.iceberg

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import spock.lang.Specification

/**
 * Validation tests for metadata files used in functional tests.
 * This ensures our test metadata files are correctly formatted.
 */
class IcebergMetadataValidationSpec extends Specification {
    
    def objectMapper = new ObjectMapper()

    def "test metadata file with branches and tags is valid JSON"() {
        given:
        def metadataPath = "../metacat-functional-tests/metacat-test-cluster/etc-metacat/data/metadata/00003-with-branches-and-tags.metadata.json"
        def file = new File(metadataPath)
        
        when:
        def json = objectMapper.readTree(file)
        
        then:
        noExceptionThrown()
        json.has("format-version")
        json.get("format-version").asInt() == 2
        json.has("refs")
        
        def refs = json.get("refs")
        refs.has("main")
        refs.has("feature-branch")
        refs.has("experimental,beta")
        refs.has("v1.0.0")
        refs.has("v2.0.0")
        refs.has("release-2024-01")
        
        refs.get("main").get("type").asText() == "branch"
        refs.get("feature-branch").get("type").asText() == "branch"
        refs.get("experimental,beta").get("type").asText() == "branch"

        refs.get("v1.0.0").get("type").asText() == "tag"
        refs.get("v2.0.0").get("type").asText() == "tag"
        refs.get("release-2024-01").get("type").asText() == "tag"
    }

    def "test metadata file with main branch only is valid JSON"() {
        given:
        def metadataPath = "../metacat-functional-tests/metacat-test-cluster/etc-metacat/data/metadata/00004-main-branch-only.metadata.json"
        def file = new File(metadataPath)
        
        when:
        def json = objectMapper.readTree(file)
        
        then:
        noExceptionThrown()
        json.has("format-version")
        json.get("format-version").asInt() == 2
        json.has("refs")
        
        def refs = json.get("refs")
        refs.has("main")
        refs.size() == 1  // Only main branch
        
        // Verify only main branch exists
        refs.get("main").get("type").asText() == "branch"
    }

    def "test metadata file created with Iceberg client < 0.14.1 (no refs section) is valid JSON"() {
        given:
        def metadataPath = "../metacat-functional-tests/metacat-test-cluster/etc-metacat/data/metadata/00005-old-client-no-refs.metadata.json"
        def file = new File(metadataPath)
        
        when:
        def json = objectMapper.readTree(file)
        
        then:
        noExceptionThrown()
        json.has("format-version")
        json.get("format-version").asInt() == 1
        
        // Tables created with Iceberg < 0.14.1 should NOT have refs section
        !json.has("refs")
        
        json.has("table-uuid")
        json.has("location")
        json.has("schema")
        json.has("current-schema-id") 
        json.has("schemas")    
        json.has("last-assigned-partition-id")
        json.has("current-snapshot-id")
        json.has("snapshots")
        
        // ~Dec 2021 (before Iceberg 0.14.1 refs support)
        json.get("last-updated-ms").asLong() == 1640909164815L
    }
    
    def "test metadata file v2 format has correct field names"() {
        given:
        def metadataPath = "../metacat-functional-tests/metacat-test-cluster/etc-metacat/data/metadata/00003-with-branches-and-tags.metadata.json"
        def file = new File(metadataPath)
        
        when:
        def json = objectMapper.readTree(file)
        
        then:
        noExceptionThrown()
        json.has("format-version")
        json.get("format-version").asInt() == 2
        
        // Critical: v2 format must have "last-partition-id" not "last-assigned-partition-id"
        json.has("last-partition-id")
        !json.has("last-assigned-partition-id")  // Wrong field name for v2
        
        // v2 format specific requirements
        json.has("last-sequence-number")
        json.has("refs")  // v2 files should have refs section
    }
    
    def "test metadata files structure matches expected branch/tag counts"() {
        expect:
        def branchesTagsFile = new File("../metacat-functional-tests/metacat-test-cluster/etc-metacat/data/metadata/00003-with-branches-and-tags.metadata.json")
        def mainOnlyFile = new File("../metacat-functional-tests/metacat-test-cluster/etc-metacat/data/metadata/00004-main-branch-only.metadata.json")
        def oldClientFile = new File("../metacat-functional-tests/metacat-test-cluster/etc-metacat/data/metadata/00005-old-client-no-refs.metadata.json")
        def v1WithRefsFile = new File("../metacat-functional-tests/metacat-test-cluster/etc-metacat/data/metadata/00006-v1-with-branches-tags.metadata.json")
        
        def branchesTagsJson = objectMapper.readTree(branchesTagsFile)
        def mainOnlyJson = objectMapper.readTree(mainOnlyFile)
        def oldClientJson = objectMapper.readTree(oldClientFile)
        def v1WithRefsJson = objectMapper.readTree(v1WithRefsFile)
        
        def branchesTagsRefs = branchesTagsJson.get("refs")
        def branchCount = 0
        def tagCount = 0
        branchesTagsRefs.fields().forEachRemaining { entry ->
            if (entry.value.get("type").asText() == "branch") {
                branchCount++
            } else if (entry.value.get("type").asText() == "tag") {
                tagCount++
            }
        }

        def mainOnlyRefs = mainOnlyJson.get("refs")
        def mainOnlyBranchCount = 0
        def mainOnlyTagCount = 0
        mainOnlyRefs.fields().forEachRemaining { entry ->
            if (entry.value.get("type").asText() == "branch") {
                mainOnlyBranchCount++
            } else if (entry.value.get("type").asText() == "tag") {
                mainOnlyTagCount++
            }
        }

        def v1WithRefsRefs = v1WithRefsJson.get("refs")
        def v1WithRefsBranchCount = 0
        def v1WithRefsTagCount = 0
        v1WithRefsRefs.fields().forEachRemaining { entry ->
            if (entry.value.get("type").asText() == "branch") {
                v1WithRefsBranchCount++
            } else if (entry.value.get("type").asText() == "tag") {
                v1WithRefsTagCount++
            }
        }

        !oldClientJson.has("refs")
        oldClientJson.get("format-version").asInt() == 1

        branchCount == 3
        tagCount == 3
        mainOnlyBranchCount == 1
        mainOnlyTagCount == 0
        v1WithRefsBranchCount == 2
        v1WithRefsTagCount == 1
    }
}
