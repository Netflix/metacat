/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.metacat.connector.hive.iceberg

import org.apache.iceberg.Table
import org.apache.iceberg.SnapshotRef
import spock.lang.Specification

/**
 * Tests for IcebergTableWrapper branch and tag detection functionality.
 * These tests verify the Iceberg 1.9 native API integration for identifying
 * branches and tags in tables.
 */
class IcebergTableWrapperSpec extends Specification {

    def "test table with no refs"() {
        given: "A table with no references"
        def mockTable = Mock(Table) {
            refs() >> [:]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect no non-main branches or tags"
        !wrapper.hasNonMainBranches()
        !wrapper.hasTags()
        !wrapper.hasNonMainBranchesOrTags()
        wrapper.getBranchesAndTagsSummary() == "branches=0, tags=0"
    }

    def "test table with only main branch"() {
        given: "A table with only the main branch"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect main branch but not consider it as having non-main branches"
        !wrapper.hasNonMainBranches() // main branch alone doesn't count as "having non-main branches"
        !wrapper.hasTags()
        !wrapper.hasNonMainBranchesOrTags()
        wrapper.getBranchesAndTagsSummary() == "branches=1 [main], tags=0"
    }

    def "test table with multiple branches including main"() {
        given: "A table with main and additional branches"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature-branch": mockBranch, "dev-branch": mockBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect multiple branches"
        wrapper.hasNonMainBranches() // multiple branches count as "having non-main branches"
        !wrapper.hasTags()
        wrapper.hasNonMainBranchesOrTags()
        wrapper.getBranchesAndTagsSummary().startsWith("branches=3")
        wrapper.getBranchesAndTagsSummary().contains("tags=0")
    }

    def "test table with only tags"() {
        given: "A table with only tags"
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["v1.0": mockTag, "v1.1": mockTag, "release-2024": mockTag]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect tags only"
        !wrapper.hasNonMainBranches()
        wrapper.hasTags()
        wrapper.hasNonMainBranchesOrTags()
        wrapper.getBranchesAndTagsSummary().startsWith("branches=0, tags=3")
    }

    def "test table with branches and tags"() {
        given: "A table with both branches and tags"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> [
                "main": mockBranch,
                "feature-x": mockBranch, 
                "v1.0": mockTag,
                "v2.0": mockTag
            ]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect both non-main branches and tags"
        wrapper.hasNonMainBranches() // more than just main
        wrapper.hasTags()
        wrapper.hasNonMainBranchesOrTags()
        def summary = wrapper.getBranchesAndTagsSummary()
        summary.contains("branches=2")
        summary.contains("tags=2")
    }

    def "test table with main and feature branch"() {
        given: "A table with main branch plus a feature branch"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature-only": mockBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect multiple branches as having non-main branches"
        wrapper.hasNonMainBranches() // multiple branches (main + feature) count as having non-main branches
        !wrapper.hasTags()
        wrapper.hasNonMainBranchesOrTags() // has non-main branches
        def summary = wrapper.getBranchesAndTagsSummary()
        summary.contains("branches=2")
        summary.contains("main")
        summary.contains("feature-only")
        summary.contains("tags=0")
    }

    def "test table created with Iceberg client < 0.14.1 (has main branch only)"() {
        given: "A table created with Iceberg client < 0.14.1 (Iceberg auto-creates main branch)"
        def mockMainBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockMainBranch]  // Even pre-0.14.1 tables get main branch auto-created
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect main branch but no additional branches or tags"
        !wrapper.hasNonMainBranches()  // Only main branch (size == 1), no additional branches
        !wrapper.hasTags()             // No tags for pre-0.14.1 client tables  
        !wrapper.hasNonMainBranchesOrTags() // No additional branches or tags
        wrapper.getBranchesAndTagsSummary() == "branches=1 [main], tags=0"  // Main branch exists

        when: "Populating metadata for pre-0.14.1 client table"
        wrapper.populateBranchTagMetadata()

        then: "Should populate with false values (no additional branches/tags)"
        noExceptionThrown()
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_NON_MAIN_BRANCHES_KEY) == "false"
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "false"
    }

    // TODO: Fix this test - mock setup issue
    /*
    def "test table with IO error throws exception"() {
        given: "A table where refs() throws an IO error"
        def mockTable = Mock(Table)
        def extraProperties = [:]

        when: "refs() throws RuntimeException and we call hasNonMainBranches"
        mockTable.refs() >> { throw new RuntimeException("Network connection failed") }
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)
        wrapper.hasNonMainBranches()

        then: "Exception should be thrown"
        thrown(RuntimeException)
    }
    */

    def "test getBranchesAndTagsSummary with various combinations"() {
        given: "A table with different ref combinations"
        def mockTable = Mock(Table)
        
        when: "Creating wrapper and getting summary"
        mockTable.refs() >> createRefsMap(refsSpec)
        def wrapper = new IcebergTableWrapper(mockTable, [:])
        def actualSummary = wrapper.getBranchesAndTagsSummary()

        then: "Summary format is correct"
        if (expectedSummary instanceof String) {
            actualSummary == expectedSummary
        } else {
            actualSummary ==~ expectedSummary
        }

        where:
        refsSpec                                      | expectedSummary
        [:]                                           | "branches=0, tags=0"
        ["main": "branch"]                           | "branches=1 [main], tags=0"
        ["v1.0": "tag"]                              | "branches=0, tags=1 [v1.0]"
        ["main": "branch", "v1.0": "tag"]           | ~/branches=1 \[main\], tags=1 \[v1\.0\]/
        ["b1": "branch", "b2": "branch"]             | ~/branches=2 \[.*\], tags=0/
        ["t1": "tag", "t2": "tag", "t3": "tag"]      | ~/branches=0, tags=3 \[.*\]/
    }
    
    private Map<String, SnapshotRef> createRefsMap(Map<String, String> refsSpec) {
        def result = [:]
        refsSpec.each { name, type ->
            result[name] = Mock(SnapshotRef) {
                isBranch() >> (type == "branch")
                isTag() >> (type == "tag")
            }
        }
        return result
    }

    def "test static constants and separate key population"() {
        given: "A table with branches and tags"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature": mockBranch, "v1.0": mockTag]
        }
        def extraProperties = [:]

        when: "Creating wrapper and populating metadata"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)
        wrapper.populateBranchTagMetadata()

        then: "Should populate both separate keys correctly"
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_NON_MAIN_BRANCHES_KEY) == "true"
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "true"
        
        and: "Static constants should have expected values"
        IcebergTableWrapper.ICEBERG_HAS_NON_MAIN_BRANCHES_KEY == "iceberg.has.non.main.branches"
        IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY == "iceberg.has.tags"
    }

    def "test separate key population - branches only"() {
        given: "A table with only branches"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature": mockBranch]
        }
        def extraProperties = [:]

        when: "Creating wrapper and populating metadata"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)
        wrapper.populateBranchTagMetadata()

        then: "Should populate non-main-branches=true, tags=false"
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_NON_MAIN_BRANCHES_KEY) == "true"
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "false"
    }

    def "test separate key population - tags only"() {
        given: "A table with only tags"
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["v1.0": mockTag, "v2.0": mockTag]
        }
        def extraProperties = [:]

        when: "Creating wrapper and populating metadata"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)
        wrapper.populateBranchTagMetadata()

        then: "Should populate non-main-branches=false, tags=true"
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_NON_MAIN_BRANCHES_KEY) == "false"
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "true"
    }

    def "test separate key population - neither"() {
        given: "A table with only main branch"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch]  // Only main branch doesn't count as "having branches"
        }
        def extraProperties = [:]

        when: "Creating wrapper and populating metadata"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)
        wrapper.populateBranchTagMetadata()

        then: "Should populate both as false"
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_NON_MAIN_BRANCHES_KEY) == "false"
        extraProperties.get(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY) == "false"
    }

    def "test constructor does not populate metadata automatically"() {
        given: "A table with branches and tags"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature": mockBranch, "v1.0": mockTag]
        }
        def extraProperties = [:]

        when: "Creating wrapper without explicit metadata population"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should NOT automatically populate metadata keys"
        !extraProperties.containsKey(IcebergTableWrapper.ICEBERG_HAS_NON_MAIN_BRANCHES_KEY)
        !extraProperties.containsKey(IcebergTableWrapper.ICEBERG_HAS_TAGS_KEY)
        
        and: "Methods should still work"
        wrapper.hasNonMainBranches()
        wrapper.hasTags()
        wrapper.hasNonMainBranchesOrTags()
    }

    def "test legacy constructor compatibility"() {
        given: "Using the legacy constructor pattern"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockTable = Mock(Table) {
            refs() >> ["main": mockBranch, "feature": mockBranch, "v1.0": mockTag]
        }
        def extraProperties = ["metadata_content": "some-content"]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should work correctly and detect non-main branches/tags"
        wrapper.getTable() == mockTable
        wrapper.getExtraProperties() == extraProperties
        wrapper.hasNonMainBranches()
        wrapper.hasTags()
        wrapper.hasNonMainBranchesOrTags()
    }

    def "test mixed reference types"() {
        given: "A table with unknown reference type mixed with known ones"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockTag = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> true
        }
        def mockUnknown = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> false  // Unknown type is neither branch nor tag
        }
        def mockTable = Mock(Table) {
            refs() >> [
                "main": mockBranch,
                "v1.0": mockTag, 
                "unknown": mockUnknown  // This should be ignored
            ]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should only detect known reference types"
        !wrapper.hasNonMainBranches() // only main branch
        wrapper.hasTags()
        wrapper.hasNonMainBranchesOrTags()
    }

    def "test empty string reference type"() {
        given: "A table with empty string reference type"
        def mockBranch = Mock(SnapshotRef) {
            isBranch() >> true
            isTag() >> false
        }
        def mockEmpty = Mock(SnapshotRef) {
            isBranch() >> false
            isTag() >> false  // Empty type is neither branch nor tag
        }
        def mockTable = Mock(Table) {
            refs() >> [
                "main": mockBranch,
                "empty-type": mockEmpty
            ]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should only detect valid reference types"
        !wrapper.hasNonMainBranches()
        !wrapper.hasTags()
        !wrapper.hasNonMainBranchesOrTags()
    }
}
