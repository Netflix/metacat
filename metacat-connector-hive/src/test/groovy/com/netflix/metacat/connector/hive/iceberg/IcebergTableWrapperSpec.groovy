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

        then: "Should detect no branches or tags"
        !wrapper.hasBranches()
        !wrapper.hasTags()
        !wrapper.hasBranchesOrTags()
        wrapper.getBranchesAndTagsSummary() == "branches=0, tags=0"
    }

    def "test table with only main branch"() {
        given: "A table with only the main branch"
        def mockTable = Mock(Table) {
            refs() >> ["main": "branch"]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect main branch but not consider it as having branches"
        !wrapper.hasBranches() // main branch alone doesn't count as "having branches"
        !wrapper.hasTags()
        !wrapper.hasBranchesOrTags()
        wrapper.getBranchesAndTagsSummary() == "branches=1 [main], tags=0"
    }

    def "test table with multiple branches including main"() {
        given: "A table with main and additional branches"
        def mockTable = Mock(Table) {
            refs() >> ["main": "branch", "feature-branch": "branch", "dev-branch": "branch"]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect multiple branches"
        wrapper.hasBranches() // multiple branches count as "having branches"
        !wrapper.hasTags()
        wrapper.hasBranchesOrTags()
        wrapper.getBranchesAndTagsSummary().startsWith("branches=3")
        wrapper.getBranchesAndTagsSummary().contains("tags=0")
    }

    def "test table with only tags"() {
        given: "A table with only tags"
        def mockTable = Mock(Table) {
            refs() >> ["v1.0": "tag", "v1.1": "tag", "release-2024": "tag"]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect tags only"
        !wrapper.hasBranches()
        wrapper.hasTags()
        wrapper.hasBranchesOrTags()
        wrapper.getBranchesAndTagsSummary().startsWith("branches=0, tags=3")
    }

    def "test table with branches and tags"() {
        given: "A table with both branches and tags"
        def mockTable = Mock(Table) {
            refs() >> [
                "main": "branch",
                "feature-x": "branch", 
                "v1.0": "tag",
                "v2.0": "tag"
            ]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect both branches and tags"
        wrapper.hasBranches() // more than just main
        wrapper.hasTags()
        wrapper.hasBranchesOrTags()
        def summary = wrapper.getBranchesAndTagsSummary()
        summary.contains("branches=2")
        summary.contains("tags=2")
    }

    def "test table with main and feature branch"() {
        given: "A table with main branch plus a feature branch"
        def mockTable = Mock(Table) {
            refs() >> ["main": "branch", "feature-only": "branch"]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should detect multiple branches as having branches"
        wrapper.hasBranches() // multiple branches (main + feature) count as having branches
        !wrapper.hasTags()
        wrapper.hasBranchesOrTags() // has branches
        def summary = wrapper.getBranchesAndTagsSummary()
        summary.contains("branches=2")
        summary.contains("main")
        summary.contains("feature-only")
        summary.contains("tags=0")
    }

    def "test table refs() throws exception"() {
        given: "A table where refs() throws an exception"
        def mockTable = Mock(Table) {
            refs() >> { throw new RuntimeException("refs not supported") }
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should handle exception gracefully and return empty sets"
        !wrapper.hasBranches()
        !wrapper.hasTags()
        !wrapper.hasBranchesOrTags()
        wrapper.getBranchesAndTagsSummary() == "branches=0, tags=0"
    }

    def "test getBranchesAndTagsSummary with various combinations"() {
        given: "A table with different ref combinations"
        def mockTable = Mock(Table)
        
        when: "Creating wrapper and getting summary"
        mockTable.refs() >> refs
        def wrapper = new IcebergTableWrapper(mockTable, [:])
        def actualSummary = wrapper.getBranchesAndTagsSummary()

        then: "Summary format is correct"
        if (expectedSummary instanceof String) {
            actualSummary == expectedSummary
        } else {
            actualSummary ==~ expectedSummary
        }

        where:
        refs                                          | expectedSummary
        [:]                                           | "branches=0, tags=0"
        ["main": "branch"]                           | "branches=1 [main], tags=0"
        ["v1.0": "tag"]                              | "branches=0, tags=1 [v1.0]"
        ["main": "branch", "v1.0": "tag"]           | ~/branches=1 \[main\], tags=1 \[v1\.0\]/
        ["b1": "branch", "b2": "branch"]             | ~/branches=2 \[.*\], tags=0/
        ["t1": "tag", "t2": "tag", "t3": "tag"]      | ~/branches=0, tags=3 \[.*\]/
    }

    def "test legacy constructor compatibility"() {
        given: "Using the legacy constructor pattern"
        def mockTable = Mock(Table) {
            refs() >> ["main": "branch", "feature": "branch", "v1.0": "tag"]
        }
        def extraProperties = ["metadata_content": "some-content"]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should work correctly and detect branches/tags"
        wrapper.getTable() == mockTable
        wrapper.getExtraProperties() == extraProperties
        wrapper.hasBranches()
        wrapper.hasTags()
        wrapper.hasBranchesOrTags()
    }

    def "test mixed reference types"() {
        given: "A table with unknown reference type mixed with known ones"
        def mockTable = Mock(Table) {
            refs() >> [
                "main": "branch",
                "v1.0": "tag", 
                "unknown": "unknown-type"  // This should be ignored
            ]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should only detect known reference types"
        !wrapper.hasBranches() // only main branch
        wrapper.hasTags()
        wrapper.hasBranchesOrTags()
    }

    def "test empty string reference type"() {
        given: "A table with empty string reference type"
        def mockTable = Mock(Table) {
            refs() >> [
                "main": "branch",
                "empty-type": ""
            ]
        }
        def extraProperties = [:]

        when: "Creating wrapper"
        def wrapper = new IcebergTableWrapper(mockTable, extraProperties)

        then: "Should only detect valid reference types"
        !wrapper.hasBranches()
        !wrapper.hasTags()
        !wrapper.hasBranchesOrTags()
    }
}
